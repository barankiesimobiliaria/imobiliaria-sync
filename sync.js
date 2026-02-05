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

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    console.error("❌ Erro: SUPABASE_URL ou SUPABASE_KEY não configuradas.");
    process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// --- FUNÇÕES AUXILIARES DE TRATAMENTO ---

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
        lerTexto(d.titulo),
        lerTexto(d.tipo),
        lerTexto(d.finalidade),
        lerTexto(d.cidade),
        lerTexto(d.bairro),
        String(Number(d.valor_venda).toFixed(2)),
        String(Number(d.valor_aluguel).toFixed(2)),
        (d.fotos_urls || []).join(',')
    ].join('|').toLowerCase();
    
    return crypto.createHash('md5').update(str).digest('hex');
}

// --- CORE DA SINCRONIZAÇÃO ---

async function runImport() {
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 SYNC À PROVA DE FALHAS - V6');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false };

    try {
        // 1. Resetar flag 'seen_today' para identificar quem saiu do XML
        console.log('🔄 Resetando flags de presença...');
        await supabase.from(TABELA_CACHE).update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        // 2. Baixar XML com Bypass de Cache (evita pegar versão antiga do servidor)
        const URL_COM_BYPASS = `${XML_URL}?t=${Date.now()}`;
        console.log(`📥 Baixando XML: ${URL_COM_BYPASS}`);
        
        const response = await axios.get(URL_COM_BYPASS);
        const parser = new XMLParser({ 
            ignoreAttributes: false, 
            attributeNamePrefix: "@_",
            trimValues: true 
        });
        
        const jsonData = parser.parse(response.data);
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        
        if (!listingsRaw) throw new Error("XML não contém a estrutura <Listing> esperada.");
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;
        console.log(`📊 Total bruto no XML: ${stats.totalXml}`);

        // 3. Carregar estado atual do banco para decidir quem atualizar
        const { data: recordsNoBanco } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id, data_hash, status')
            .eq('xml_provider', PROVIDER_NAME);

        const mapaBanco = new Map(recordsNoBanco.map(r => [String(r.listing_id), r]));
        const agora = new Date().toISOString();
        const idsProcessados = new Set();

        // 4. Processar em Batches
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id || idsProcessados.has(listing_id)) continue;
                idsProcessados.add(listing_id);

                const details = item.Details || {};
                const location = item.Location || {};
                const transacao = lerTexto(item.TransactionType);

                // Montagem do objeto (Regra: se está no XML, status = 'ativo')
                const dadosImovel = {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    tipo: lerTexto(details.PropertyType),
                    finalidade: transacao,
                    status: 'ativo', 
                    endereco: lerTexto(location.Address),
                    cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood),
                    uf: lerTexto(location.State) || 'PR',
                    quartos: parseInt(lerValor(details.Bedrooms)) || 0,
                    suites: parseInt(lerValor(details.Suites)) || 0,
                    banheiros: parseInt(lerValor(details.Bathrooms)) || 0,
                    vagas_garagem: parseInt(lerValor(details.Garage)) || 0,
                    area_total: lerValor(details.LotArea),
                    area_util: lerValor(details.LivingArea),
                    valor_venda: lerValor(details.ListPrice),
                    valor_aluguel: lerValor(details.RentalPrice),
                    valor_condominio: lerValor(details.PropertyAdministrationFee),
                    descricao: lerTexto(details.Description),
                    seen_today: true,
                    last_sync: agora,
                    xml_provider: PROVIDER_NAME
                };

                const hashNovo = gerarHash(dadosImovel);
                dadosImovel.data_hash = hashNovo;

                const registroAntigo = mapaBanco.get(listing_id);

                // Lógica de Decisão Crítica:
                if (!registroAntigo) {
                    // Imóvel novo
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else if (registroAntigo.status === 'inativo' || registroAntigo.data_hash !== hashNovo) {
                    // Reativação ou Mudança de dados: Força atualização completa
                    stats.atualizados++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    // Sem mudanças: Apenas marca presença e garante status ativo
                    stats.semAlteracao++;
                    upsertData.push({ 
                        listing_id, 
                        seen_today: true, 
                        last_sync: agora, 
                        status: 'ativo' 
                    });
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: 'listing_id' });
                if (error) console.error(`❌ Erro no batch em ${upsertData[0].listing_id}:`, error.message);
            }
        }

        console.log(`✨ IDs únicos processados: ${idsProcessados.size}`);

        // 5. Inativar quem não apareceu no XML
        console.log('🗑️ Verificando imóveis para inativação...');
        const { data: paraInativar } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id')
            .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });

        if (paraInativar && paraInativar.length > 0) {
            const idsDesativar = paraInativar.map(p => p.listing_id);
            await supabase
                .from(TABELA_CACHE)
                .update({ status: 'inativo', data_ultima_alteracao: agora })
                .in('listing_id', idsDesativar);
            stats.desativados = idsDesativar.length;
            console.log(`📉 Inativados: ${stats.desativados} imóveis.`);
        }

        console.log('✅ SUCESSO!');
        console.log(`Resumo: ${stats.novos} novos, ${stats.atualizados} atualizados, ${stats.semAlteracao} mantidos.`);

    } catch (error) {
        console.error('💥 ERRO CRÍTICO:', error.message);
        process.exit(1);
    }
}

runImport();
