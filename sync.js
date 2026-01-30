require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    console.error("❌ Erro: SUPABASE_URL ou SUPABASE_KEY não configuradas.");
    process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false }
});

// --- FUNÇÕES AUXILIARES ---
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
        d.titulo || '', d.tipo || '', d.finalidade || '', d.cidade || '', d.bairro || '', d.endereco || '',
        String(d.quartos || 0), String(d.suites || 0), String(d.banheiros || 0), String(d.vagas_garagem || 0),
        String(d.area_total || 0), String(d.area_util || 0), String(d.valor_venda || 0), String(d.valor_aluguel || 0),
        String(d.valor_condominio || 0), d.descricao || '', JSON.stringify(d.fotos_urls || [])
    ].join('|');
    return crypto.createHash('md5').update(str).digest('hex');
}

async function runImport() {
    console.log('🚀 INICIANDO SINCRONIZAÇÃO V5 - PROTEÇÃO TOTAL');
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false, mensagemErro: null };

    try {
        // 1. Buscar hashes e status atuais
        const { data: existentes } = await supabase.from('cache_xml_externo').select('listing_id, data_hash, status').eq('xml_provider', PROVIDER_NAME);
        const dbMap = new Map((existentes || []).map(e => [e.listing_id, { hash: e.data_hash, status: e.status }]));

        // 2. Baixar XML (Fazemos o download ANTES de qualquer alteração no banco)
        const response = await axios.get(XML_URL, { timeout: 120000, responseType: 'text' });
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou inválido");
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        // 3. Resetar flags seen_today APENAS se o XML foi baixado com sucesso
        await supabase.from('cache_xml_externo').update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        // 4. Processar Imóveis
        const agora = new Date().toISOString();
        const idsNoXml = new Set();

        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];
            
            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id) continue;
                idsNoXml.add(listing_id);

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

                const dadosBase = {
                    listing_id, titulo: lerTexto(item.Title), tipo: lerTexto(details.PropertyType), finalidade: transacao,
                    status: 'ativo', endereco: lerTexto(location.Address), cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood), uf: lerTexto(location.State) || 'PR',
                    latitude: location.Latitude ? String(location.Latitude) : null, longitude: location.Longitude ? String(location.Longitude) : null,
                    quartos: parseInt(lerValor(details.Bedrooms)) || 0, suites: parseInt(lerValor(details.Suites)) || 0,
                    banheiros: parseInt(lerValor(details.Bathrooms)) || 0, vagas_garagem: parseInt(lerValor(details.Garage)) || 0,
                    area_total: lerValor(details.LotArea), area_util: lerValor(details.LivingArea),
                    valor_venda: vVenda, valor_aluguel: vAluguel, valor_condominio: lerValor(details.PropertyAdministrationFee),
                    iptu: lerValor(details.YearlyTax) || lerValor(details.MonthlyTax), descricao: lerTexto(details.Description),
                    diferenciais: lerFeatures(details.Features), fotos_urls: fotos, seen_today: true, last_sync: agora, xml_provider: PROVIDER_NAME
                };

                const hashNovo = gerarHash(dadosBase);
                dadosBase.data_hash = hashNovo;
                const registroExistente = dbMap.get(listing_id);

                if (!registroExistente) {
                    stats.novos++;
                    dadosBase.data_ultima_alteracao = agora;
                    upsertData.push(dadosBase);
                } else if (registroExistente.hash !== hashNovo) {
                    stats.atualizados++;
                    dadosBase.data_ultima_alteracao = agora;
                    upsertData.push(dadosBase);
                } else {
                    stats.semAlteracao++;
                    // SEMPRE enviamos o status 'ativo' e 'seen_today' para garantir paridade
                    upsertData.push({ listing_id, status: 'ativo', seen_today: true, last_sync: agora });
                }
            }
            if (upsertData.length > 0) {
                const { error } = await supabase.from('cache_xml_externo').upsert(upsertData, { onConflict: 'listing_id' });
                if (error) console.error(`   ❌ Erro batch: ${error.message}`);
            }
        }

        // 5. Inativar removidos (PROTEÇÃO: Só inativa se processamos todos os IDs do XML)
        if (idsNoXml.size >= stats.totalXml) {
            const { data: desativados } = await supabase
                .from('cache_xml_externo')
                .update({ status: 'inativo' })
                .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' })
                .select('listing_id');
            stats.desativados = desativados ? desativados.length : 0;
        } else {
            console.error('⚠️ Aviso: Processamento incompleto. Inativação cancelada para segurança.');
        }

        // 6. Registrar Log
        await supabase.from('import_logs').insert({
            data_execucao: agora, status: 'sucesso', total_xml: stats.totalXml,
            novos: stats.novos, atualizados: stats.atualizados, removidos: stats.desativados, sem_alteracao: stats.semAlteracao
        });

        console.log('✅ Sincronização concluída!');
    } catch (error) {
        console.error('💥 Erro:', error.message);
        await supabase.from('import_logs').insert({ data_execucao: new Date().toISOString(), status: 'erro', mensagem_erro: error.message });
        process.exit(1);
    }
}
runImport();
