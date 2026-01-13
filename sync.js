require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

function lerValor(campo) {
    if (!campo) return 0;
    if (typeof campo === 'object') return campo['#text'] ? parseFloat(campo['#text']) : 0;
    return parseFloat(campo) || 0;
}

function lerTexto(campo) {
    if (!campo) return '';
    if (typeof campo === 'object') return campo['#text'] || '';
    return String(campo);
}

function lerFeatures(featuresNode) {
    if (!featuresNode || !featuresNode.Feature) return [];
    const feat = featuresNode.Feature;
    const lista = Array.isArray(feat) ? feat : [feat];
    return lista.map(f => lerTexto(f));
}

async function runImport() {
    console.log(`[${new Date().toISOString()}] Iniciando Importação V8 (Coords + Capa + Titulo)...`);
    let stats = { total: 0, processados: 0, erros: 0 };

    try {
        console.log('1. Baixando XML...');
        const response = await axios.get(XML_URL, { responseType: 'text' });
        
        console.log('2. Parseando...');
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        
        if (!jsonData['ListingDataFeed'] || !jsonData['ListingDataFeed']['Listings']) {
            throw new Error("Estrutura do XML inválida ou vazia.");
        }

        const listingsRaw = jsonData['ListingDataFeed']['Listings']['Listing'];
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.total = listings.length;

        const BATCH_SIZE = 50;
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                try {
                    const details = item.Details || {};
                    const location = item.Location || {};
                    const transacao = lerTexto(item.TransactionType); 
                    const tipoImovel = lerTexto(details.PropertyType);

                    // --- TRATAMENTO DE PREÇOS ---
                    let vVenda = 0, vAluguel = 0;
                    const rawListPrice = lerValor(details.ListPrice);
                    const rawRentalPrice = lerValor(details.RentalPrice);

                    if (transacao === 'For Rent') vAluguel = rawRentalPrice > 0 ? rawRentalPrice : rawListPrice;
                    else if (transacao === 'For Sale') vVenda = rawListPrice;
                    else { vVenda = rawListPrice; vAluguel = rawRentalPrice; }

                    // --- TRATAMENTO DE FOTOS ---
                    let mediaItems = [];
                    if (item.Media && item.Media.Item) {
                        mediaItems = Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item];
                    }
                    let listaFotos = [];
                    let fotoCapa = null;
                    mediaItems.forEach(m => {
                        const url = lerTexto(m);
                        if (url && url.length > 0 && url.startsWith('http')) {
                            const isPrimary = (m['@_primary'] === 'true' || m['@_primary'] === true);
                            if (isPrimary) fotoCapa = url;
                            else listaFotos.push(url);
                        }
                    });
                    if (fotoCapa) listaFotos.unshift(fotoCapa);

                    // --- NOVO: Captura de Coordenadas ---
                    const lat = location.Latitude ? lerTexto(location.Latitude) : null;
                    const lon = location.Longitude ? lerTexto(location.Longitude) : null;

                    // --- INFORMAÇÕES SANITIZADAS ---
                    let banheiros = parseInt(lerValor(details.Bathrooms)) || 0;
                    let vagas = parseInt(lerValor(details.Garage)) || 0;
                    const isResidencial = tipoImovel.includes('Residential');
                    if (isResidencial) {
                        if (banheiros > 10) banheiros = 10;
                        if (vagas > 20) vagas = 20;
                    }

                    upsertData.push({
                        listing_id: lerTexto(item.ListingID),
                        titulo: lerTexto(item.Title),
                        tipo: tipoImovel,
                        finalidade: transacao,
                        status: 'ativo',
                        cidade: location.City ? lerTexto(location.City).toUpperCase() : null,
                        bairro: lerTexto(location.Neighborhood),
                        uf: lerTexto(location.State) || 'PR',
                        
                        // NOVOS CAMPOS AQUI
                        latitude: lat,
                        longitude: lon,
                        
                        quartos: parseInt(lerValor(details.Bedrooms)) || 0,
                        suites: parseInt(lerValor(details.Suites)) || 0,
                        banheiros: banheiros,
                        vagas_garagem: vagas,
                        area_total: lerValor(details.LotArea),
                        area_util: lerValor(details.LivingArea),
                        
                        valor_venda: vVenda,
                        valor_aluguel: vAluguel,
                        valor_condominio: lerValor(details.PropertyAdministrationFee),
                        iptu: lerValor(details.YearlyTax) || lerValor(details.MonthlyTax),
                        
                        descricao: lerTexto(details.Description),
                        diferenciais: lerFeatures(details.Features),
                        fotos_urls: listaFotos,
                        
                        seen_today: true,
                        last_sync: new Date(),
                        xml_provider: 'RedeUrbana'
                    });

                } catch (e) {
                    console.error(`Erro item ${item.ListingID}:`, e.message);
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from('cache_xml_externo').upsert(upsertData, { 
                    onConflict: 'listing_id' 
                });
                if (!error) stats.processados += upsertData.length;
                else console.error('Erro lote Supabase:', error.message);
            }
            if (i % 500 === 0) console.log(`Progresso: ${i}/${stats.total}...`);
        }
        console.log('✅ Importação V8 Finalizada com Sucesso!');
    } catch (error) { console.error('❌ Erro Fatal:', error.message); }
}

runImport();
