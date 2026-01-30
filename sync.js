require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

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

// Função para registrar o log de importação
async function registrarLog(stats) {
    try {
        const { error } = await supabase.from('import_logs').insert({
            data_execucao: new Date().toISOString(),
            status: stats.erro ? 'erro' : 'sucesso',
            total_xml: stats.totalXml,
            novos: stats.novos,
            atualizados: stats.atualizados,
            removidos: stats.desativados,
            mensagem_erro: stats.mensagemErro || null
        });
        
        if (error) {
            console.error('⚠️ Erro ao salvar log:', error.message);
        } else {
            console.log('📝 Log de importação registrado com sucesso!');
        }
    } catch (err) {
        console.error('⚠️ Falha ao registrar log:', err.message);
    }
}

async function runImport() {
    console.log(`🚀 Iniciando Sincronização...`);
    let stats = { 
        totalXml: 0, 
        novos: 0, 
        atualizados: 0, 
        desativados: 0, 
        erros: 0,
        erro: false,
        mensagemErro: null
    };

    try {
        // Pegar IDs existentes para saber quais são novos
        const { data: existentes } = await supabase
            .from('cache_xml_externo')
            .select('listing_id')
            .eq('xml_provider', PROVIDER_NAME);
        
        const idsExistentes = new Set((existentes || []).map(e => e.listing_id));

        console.log(`0. Resetando flags para: ${PROVIDER_NAME}`);
        await supabase.from('cache_xml_externo').update({ seen_today: false }).eq('xml_provider', PROVIDER_NAME);

        console.log('1. Baixando XML...');
        const response = await axios.get(XML_URL, { timeout: 60000, responseType: 'text' });
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou inválido.");
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        console.log('2. Processando imóveis...');
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = batch.map(item => {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id) return null;

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
                    if (url.startsWith('http')) {
                        if ((m['@_primary'] === 'true' || m['@_primary'] === true) && !capa) capa = url;
                        else fotos.push(url);
                    }
                });
                if (capa) fotos.unshift(capa);

                // Verificar se é novo ou atualização
                const isNovo = !idsExistentes.has(listing_id);
                if (isNovo) stats.novos++;
                else stats.atualizados++;

                return {
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
                    last_sync: new Date().toISOString(),
                    xml_provider: PROVIDER_NAME
                };
            }).filter(x => x !== null);

            if (upsertData.length > 0) {
                const { error } = await supabase.from('cache_xml_externo').upsert(upsertData, { onConflict: 'listing_id' });
                if (error) {
                    stats.erros += upsertData.length;
                    console.error('Erro no batch:', error.message);
                }
            }
            
            // Progresso
            console.log(`   Processado: ${Math.min(i + BATCH_SIZE, listings.length)}/${listings.length}`);
        }

        console.log('3. Inativando removidos...');
        const { data: desat } = await supabase.from('cache_xml_externo')
            .update({ status: 'inativo' })
            .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' })
            .select();
        
        stats.desativados = desat ? desat.length : 0;
        
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`✅ SINCRONIZAÇÃO CONCLUÍDA!`);
        console.log(`   📊 Total no XML: ${stats.totalXml}`);
        console.log(`   🆕 Novos: ${stats.novos}`);
        console.log(`   🔄 Atualizados: ${stats.atualizados}`);
        console.log(`   ❌ Desativados: ${stats.desativados}`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

        // Registrar log de sucesso
        await registrarLog(stats);

    } catch (error) {
        console.error('💥 Erro Fatal:', error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        
        // Registrar log de erro
        await registrarLog(stats);
        
        process.exit(1);
    }
}

runImport();
