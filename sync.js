require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;
const TABELA_CACHE = 'cache_xml_externo';
const TABELA_LOGS = 'import_logs';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// Funções auxiliares (Mantidas conforme seu original)
function lerValor(campo) {
    if (campo === undefined || campo === null) return 0;
    if (typeof campo === 'object') return campo['#text'] ? parseFloat(campo['#text']) : 0;
    const val = parseFloat(campo);
    return isNaN(val) ? 0 : val;
}

function lerTexto(campo) {
    if (!campo) return '';
    if (typeof campo === 'object') return campo['#text'] || '';
    return String(campo).trim();
}

function lerFeatures(featuresNode) {
    if (!featuresNode || !featuresNode.Feature) return [];
    const feat = featuresNode.Feature;
    const lista = Array.isArray(feat) ? feat : [feat];
    return lista.map(f => lerTexto(f)).filter(f => f !== '').sort();
}

function gerarHash(d) {
    const str = [
        lerTexto(d.titulo), lerTexto(d.tipo), lerTexto(d.finalidade),
        lerTexto(d.cidade), lerTexto(d.bairro),
        String(Number(d.valor_venda).toFixed(2)),
        String(Number(d.valor_aluguel).toFixed(2)),
        (d.fotos_urls || []).join(',')
    ].join('|').toLowerCase();
    return crypto.createHash('md5').update(str).digest('hex');
}

async function runImport() {
    console.log('🚀 INICIANDO SINCRONIZAÇÃO À PROVA DE FALHAS');
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false };

    try {
        // 1. Resetar flag de todos para este provedor
        await supabase.from(TABELA_CACHE).update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        // 2. Baixar XML
        const response = await axios.get(XML_URL);
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        // 3. Buscar Status Atual no Banco para Comparação
        const { data: recordsNoBanco } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id, data_hash, status')
            .eq('xml_provider', PROVIDER_NAME);

        const mapaBanco = new Map(recordsNoBanco.map(r => [r.listing_id, r]));
        const agora = new Date().toISOString();

        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id) continue;

                // Extração de dados (simplificada para o exemplo)
                const details = item.Details || {};
                const location = item.Location || {};
                
                const dadosImovel = {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    tipo: lerTexto(details.PropertyType),
                    finalidade: lerTexto(item.TransactionType),
                    status: 'ativo', // 💡 REGRA DE OURO: Se está no XML, o status deve ser ATIVO
                    endereco: lerTexto(location.Address),
                    cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood),
                    valor_venda: lerValor(details.ListPrice),
                    valor_aluguel: lerValor(details.RentalPrice),
                    seen_today: true,
                    last_sync: agora,
                    xml_provider: PROVIDER_NAME
                };

                const hashNovo = gerarHash(dadosImovel);
                dadosImovel.data_hash = hashNovo;

                const registroNoBanco = mapaBanco.get(listing_id);

                // LÓGICA DE DECISÃO:
                if (!registroNoBanco) {
                    // Novo imóvel
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else if (registroNoBanco.status === 'inativo' || registroNoBanco.data_hash !== hashNovo) {
                    // 💡 CORREÇÃO: Se estiver inativo OU o hash mudou, força atualização completa
                    stats.atualizados++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    // Tudo igual, apenas marca que viu ele hoje
                    stats.semAlteracao++;
                    upsertData.push({ listing_id, seen_today: true, last_sync: agora, status: 'ativo' });
                }
            }

            if (upsertData.length > 0) {
                await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: 'listing_id' });
            }
        }

        // 4. INATIVAÇÃO GLOBAL (Quem não foi visto hoje)
        // 💡 REMOVIDA a trava de "status: ativo" para garantir que nada escape
        const { data: paraInativar } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id')
            .match({ xml_provider: PROVIDER_NAME, seen_today: false });

        if (paraInativar && paraInativar.length > 0) {
            const idsInativar = paraInativar.map(p => p.listing_id);
            await supabase
                .from(TABELA_CACHE)
                .update({ status: 'inativo', data_ultima_alteracao: agora })
                .in('listing_id', idsInativar);
            stats.desativados = idsInativar.length;
        }

        console.log(`✅ Sincronismo Finalizado: ${stats.totalXml} no XML | ${stats.desativados} Inativados`);
        
    } catch (error) {
        console.error('💥 ERRO CRÍTICO:', error.message);
        process.exit(1);
    }
}

runImport();
