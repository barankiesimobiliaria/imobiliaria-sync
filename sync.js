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

// Configurações de Robustez
const MAX_RETRIES = 3;
const AXIOS_TIMEOUT = 60000; // 60 segundos para download de arquivos grandes

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    console.error("❌ Erro: SUPABASE_URL ou SUPABASE_KEY não configuradas.");
    process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false }
});

// Funções Auxiliares de Leitura
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

/**
 * Gera um Hash robusto focado apenas nos dados vitais.
 */
function gerarHash(d) {
    const str = [
        lerTexto(d.titulo),
        lerTexto(d.tipo),
        lerTexto(d.finalidade),
        lerTexto(d.cidade),
        lerTexto(d.bairro),
        lerTexto(d.endereco),
        String(parseInt(d.quartos) || 0),
        String(parseInt(d.suites) || 0),
        String(parseInt(d.banheiros) || 0),
        String(parseInt(d.vagas_garagem) || 0),
        String(Number(d.area_total).toFixed(2)),
        String(Number(d.area_util).toFixed(2)),
        String(Number(d.valor_venda).toFixed(2)),
        String(Number(d.valor_aluguel).toFixed(2)),
        String(Number(d.valor_condominio).toFixed(2)),
        lerTexto(d.descricao).substring(0, 500),
        (d.fotos_urls || []).join(',')
    ].join('|').toLowerCase();
    
    return crypto.createHash('md5').update(str).digest('hex');
}

/**
 * Função de download com Retry para evitar falhas de rede
 */
async function downloadXML(url, retries = MAX_RETRIES) {
    for (let i = 0; i < retries; i++) {
        try {
            console.log(`📥 Baixando XML (Tentativa ${i + 1}/${retries})...`);
            const response = await axios.get(url, { 
                timeout: AXIOS_TIMEOUT,
                headers: { 'Accept-Encoding': 'gzip, deflate, br' } // Melhora performance de download
            });
            if (!response.data) throw new Error("Resposta do servidor vazia");
            return response.data;
        } catch (err) {
            console.error(`⚠️ Falha no download: ${err.message}`);
            if (i === retries - 1) throw err;
            const wait = Math.pow(2, i) * 1000; // Exponential backoff
            await new Promise(res => setTimeout(res, wait));
        }
    }
}

async function buscarDadosExistentes() {
    console.log('   Buscando dados existentes...');
    const mapa = new Map();
    let offset = 0;
    const limite = 1000;
    
    while (true) {
        const { data, error } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id, data_hash, xml_provider, status') 
            .range(offset, offset + limite - 1);
        
        if (error) {
            console.error(`   Erro na busca:`, error.message);
            throw error;
        }
        
        if (!data || data.length === 0) break;
        
        data.forEach(item => {
            mapa.set(String(item.listing_id).trim(), {
                hash: item.data_hash || '',
                provider: item.xml_provider,
                status: item.status
            });
        });
        
        if (data.length < limite) break;
        offset += limite;
    }
    
    console.log(`   ✅ ${mapa.size} registros carregados`);
    return mapa;
}

async function registrarLog(stats) {
    try {
        await supabase.from(TABELA_LOGS).insert({
            data_execucao: new Date().toISOString(),
            status: stats.erro ? 'erro' : 'sucesso',
            total_xml: stats.totalXml,
            novos: stats.novos,
            atualizados: stats.atualizados,
            removidos: stats.desativados,
            sem_alteracao: stats.semAlteracao,
            mensagem_erro: stats.mensagemErro || null
        });
        console.log('📝 Log registrado!');
    } catch (err) {
        console.error('⚠️ Erro ao salvar log:', err.message);
    }
}

async function runImport() {
    console.log('');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 SINCRONIZAÇÃO XML OTIMIZADA - V6');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false, mensagemErro: null };

    try {
        const dadosExistentes = await buscarDadosExistentes();

        console.log('🔄 Resetando flags de presença...');
        // Resetamos apenas a flag seen_today, o status permanece até o fim do processo
        await supabase.from(TABELA_CACHE).update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        const xmlData = await downloadXML(XML_URL);
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(xmlData);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("Estrutura do XML inválida ou sem imóveis");
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;
        console.log(`📦 Processando ${stats.totalXml} imóveis do XML...`);

        const idsProcessadosNoXML = new Set();
        const agora = new Date().toISOString();
        
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];
            
            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id || idsProcessadosNoXML.has(listing_id)) continue;
                idsProcessadosNoXML.add(listing_id);

                const details = item.Details || {};
                const location = item.Location || {};
                const transacao = lerTexto(item.TransactionType);
                
                let vVenda = 0, vAluguel = 0;
                const pVenda = lerValor(details.ListPrice);
                const pAluguel = lerValor(details.RentalPrice);
                if (transacao === 'For Rent') vAluguel = pAluguel || pVenda;
                else if (transacao === 'For Sale') vVenda = pVenda;
                else { vVenda = pVenda; vAluguel = pAluguel; }

                let mediaItems = item.Media?.Item ? (Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item]) : [];
                let fotos = [];
                let capa = null;
                mediaItems.forEach(m => {
                    const url = lerTexto(m);
                    if (url && url.startsWith('http')) {
                        if ((m['@_primary'] === 'true' || m['@_primary'] === true) && !capa) capa = url;
                        else fotos.push(url);
                    }
                });
                if (capa) fotos.unshift(capa);

                const dadosImovel = {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    tipo: lerTexto(details.PropertyType),
                    finalidade: transacao,
                    status: 'ativo', // Se está no XML, deve estar ativo
                    endereco: lerTexto(location.Address),
                    cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood),
                    uf: lerTexto(location.State) || 'PR',
                    latitude: location.Latitude ? String(location.Latitude) : null,
                    longitude: location.Longitude ? String(location.Longitude) : null,
                    quartos: parseInt(lerValor(details.Bedrooms)) || 0,
                    suites: parseInt(lerValor(details.Suites)) || 0,
                    banheiros: parseInt(lerValor(details.Bathrooms)) || 0,
                    vagas_garagem: parseInt(lerValor(details.Garage)) || 0,
                    area_total: lerValor(details.LotArea),
                    area_util: lerValor(details.LivingArea),
                    valor_venda: vVenda,
                    valor_aluguel: vAluguel,
                    valor_condominio: lerValor(details.PropertyAdministrationFee),
                    iptu: lerValor(details.YearlyTax) || lerValor(details.MonthlyTax),
                    descricao: lerTexto(details.Description),
                    diferenciais: lerFeatures(details.Features),
                    fotos_urls: fotos,
                    seen_today: true,
                    last_sync: agora,
                    xml_provider: PROVIDER_NAME
                };

                const hashNovo = gerarHash(dadosImovel);
                dadosImovel.data_hash = hashNovo;

                const registroAntigo = dadosExistentes.get(listing_id);
                
                // Lógica de Decisão: Novo, Atualizado ou Sem Alteração
                if (!registroAntigo) {
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    const mudouHash = registroAntigo.hash !== hashNovo;
                    const estavaInativo = registroAntigo.status !== 'ativo';
                    
                    if (mudouHash || estavaInativo) {
                        stats.atualizados++;
                        dadosImovel.data_ultima_alteracao = agora;
                        upsertData.push(dadosImovel);
                    } else {
                        stats.semAlteracao++;
                        // Apenas marca como visto e atualiza o last_sync
                        upsertData.push({ listing_id, seen_today: true, last_sync: agora, status: 'ativo' });
                    }
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: 'listing_id' });
                if (error) throw new Error(`Erro no upsert batch: ${error.message}`);
            }
        }

        console.log('🗑️ Inativando imóveis ausentes no XML...');
        // Inativa quem é do provedor, estava ativo e não foi visto hoje
        const { error: errorInativar, count: desativadosCount } = await supabase
            .from(TABELA_CACHE)
            .update({ status: 'inativo', data_ultima_alteracao: agora })
            .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });
        
        if (errorInativar) console.error("⚠️ Erro ao inativar ausentes:", errorInativar.message);
        stats.desativados = desativadosCount || 0;
        
        console.log(`✅ CONCLUÍDO! Novos: ${stats.novos}, Atualizados: ${stats.atualizados}, Inativados: ${stats.desativados}`);
        await registrarLog(stats);

    } catch (error) {
        console.error('💥 ERRO FATAL:', error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        await registrarLog(stats);
        process.exit(1);
    }
}

runImport();
