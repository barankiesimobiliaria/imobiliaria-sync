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

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false }
});

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
    return lista.map(f => lerTexto(f)).filter(f => f !== '');
}

function gerarHash(d) {
    const str = [
        d.titulo || '',
        d.tipo || '',
        d.finalidade || '',
        d.cidade || '',
        d.bairro || '',
        d.endereco || '',
        String(d.quartos || 0),
        String(d.suites || 0),
        String(d.banheiros || 0),
        String(d.vagas_garagem || 0),
        String(d.area_total || 0),
        String(d.area_util || 0),
        String(d.valor_venda || 0),
        String(d.valor_aluguel || 0),
        String(d.valor_condominio || 0),
        d.descricao || '',
        JSON.stringify(d.fotos_urls || [])
    ].join('|');
    return crypto.createHash('md5').update(str).digest('hex');
}

async function buscarDadosExistentes() {
    console.log('   Buscando dados existentes...');
    const mapa = new Map();
    let offset = 0;
    const limite = 1000;
    
    while (true) {
        // MUDANÇA CRÍTICA: Removemos o filtro de xml_provider na busca inicial.
        // Se o imóvel já existe no banco (mesmo que por outro provider), 
        // o script deve reconhecê-lo para evitar erro de duplicidade no upsert.
        const { data, error } = await supabase
            .from(TABELA_CACHE)
            .select('listing_id, data_hash, xml_provider') 
            .range(offset, offset + limite - 1);
        
        if (error) {
            console.error(`   Erro na busca:`, error.message);
            throw error;
        }
        
        if (!data || data.length === 0) break;
        
        data.forEach(item => {
            mapa.set(String(item.listing_id).trim(), {
                hash: item.data_hash || '',
                provider: item.xml_provider
            });
        });
        
        if (data.length < limite) break;
        offset += limite;
    }
    
    console.log(`   ✅ ${mapa.size} registros carregados no cache global`);
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
    console.log('🚀 SINCRONIZAÇÃO XML - V4 (GLOBAL CACHE)');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false, mensagemErro: null };

    try {
        const dadosExistentes = await buscarDadosExistentes();

        console.log('🔄 Resetando flags...');
        await supabase.from(TABELA_CACHE).update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        console.log('📥 Baixando XML...');
        const response = await axios.get(XML_URL);
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou inválido");
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        const idsProcessados = new Set();
        const agora = new Date().toISOString();
        
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
                    status: 'ativo',
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
                
                // Lógica de decisão:
                // Se não existe (mesmo em outros providers), é NOVO.
                // Se existe mas o provider é diferente, é uma ATUALIZAÇÃO (troca de dono/xml).
                // Se existe e o hash é diferente, é ATUALIZADO.
                if (!registroAntigo) {
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else if (registroAntigo.provider !== PROVIDER_NAME || registroAntigo.hash !== hashNovo) {
                    stats.atualizados++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    stats.semAlteracao++;
                    upsertData.push({ listing_id, seen_today: true, last_sync: agora, status: 'ativo' });
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: 'listing_id' });
                if (error) throw new Error(`Erro no upsert: ${error.message}`);
            }
        }

        console.log('🗑️ Inativando removidos...');
        const { data: paraDesativar } = await supabase.from(TABELA_CACHE).select('listing_id').match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });
        if (paraDesativar && paraDesativar.length > 0) {
            await supabase.from(TABELA_CACHE).update({ status: 'inativo', data_ultima_alteracao: agora }).match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });
            stats.desativados = paraDesativar.length;
        }
        
        console.log('✅ CONCLUÍDO!');
        await registrarLog(stats);

    } catch (error) {
        console.error('💥 ERRO:', error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        await registrarLog(stats);
        process.exit(1);
    }
}

runImport();
