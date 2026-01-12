require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

function lerValor(campo) {
    if (!campo) return 0;
    if (typeof campo === 'object') {
        return campo['#text'] ? parseFloat(campo['#text']) : 0;
    }
    return parseFloat(campo) || 0;
}

function lerTexto(campo) {
    if (!campo) return '';
    if (typeof campo === 'object') {
        return campo['#text'] || '';
    }
    return String(campo);
}

function lerFeatures(featuresNode) {
    if (!featuresNode || !featuresNode.Feature) return [];
    const feat = featuresNode.Feature;
    const lista = Array.isArray(feat) ? feat : [feat];
    return lista.map(f => lerTexto(f));
}

async function runImport() {
    console.log(`[${new Date().toISOString()}] Iniciando Importação V6 (Hard Reset & Sanity Check)...`);
    let stats = { total: 0, processados: 0, erros: 0 };

    try {
        console.log('1. Baixando XML...');
        const response = await axios.get(XML_URL, { responseType: 'text' });
        
        console.log('2. Parseando...');
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
        const jsonData = parser.parse(response.data);
        const listings = jsonData['ListingDataFeed']['Listings']['Listing'];
        stats.total = listings.length;

        const BATCH_SIZE = 50;
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                try {
                    const details = item.Details || {};
                    const location = item.Location || {};
                    const transacao = lerTexto(item.TransactionType); // "For Sale" ou "For Rent"
                    const tipoImovel = lerTexto(details.PropertyType);

                    // --- LÓGICA RÍGIDA DE PREÇOS ---
                    let vVenda = 0;
                    let vAluguel = 0;
                    
                    const rawListPrice = lerValor(details.ListPrice);
                    const rawRentalPrice = lerValor(details.RentalPrice);

                    if (transacao === 'For Rent') {
                        // Se é aluguel, SÓ aceita aluguel. Venda vira 0.
                        vAluguel = rawRentalPrice > 0 ? rawRentalPrice : rawListPrice;
                        vVenda = 0; 
                    } else if (transacao === 'For Sale') {
                        // Se é venda, SÓ aceita venda. Aluguel vira 0.
                        vVenda = rawListPrice;
                        vAluguel = 0;
                    } else {
                        // Se for "Sale/Rent" (venda e aluguel), tenta pegar os dois
                        vVenda = rawListPrice;
                        vAluguel = rawRentalPrice;
                    }

                    // --- SANITY CHECK (Evitar 43 banheiros) ---
                    let banheiros = parseInt(lerValor(details.Bathrooms)) || 0;
                    let vagas = parseInt(lerValor(details.Garage)) || 0;
                    
                    // Se for apartamento/casa residencial e tiver valores absurdos, limita
                    const isResidencial = tipoImovel.includes('Residential') || tipoImovel.includes('Apartment') || tipoImovel.includes('Home');
                    
                    if (isResidencial) {
                        if (banheiros > 10) banheiros = 10; // Cap de segurança
                        if (vagas > 20) vagas = 20; // Cap de segurança
                    }

                    // Fotos
                    let mediaItems = [];
                    if (item.Media && item.Media.Item) {
                        mediaItems = Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item];
                    }
                    const fotos = mediaItems.map(m => lerTexto(m)).filter(f => f.length > 0 && f.startsWith('http'));

                    upsertData.push({
                        listing_id: lerTexto(item.ListingID),
                        tipo: tipoImovel,
                        finalidade: transacao,
                        status: 'ativo',
                        cidade: location.City ? lerTexto(location.City).toUpperCase() : null,
                        bairro: lerTexto(location.Neighborhood),
                        uf: lerTexto(location.State) || 'PR',
                        
                        quartos: parseInt(lerValor(details.Bedrooms)) || 0,
                        suites: parseInt(lerValor(details.Suites)) || 0,
                        banheiros: banheiros, // Valor sanitizado
                        vagas_garagem: vagas, // Valor sanitizado
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
                
                if (!error) {
                    stats.processados += upsertData.length;
                } else {
                    console.error('Erro lote:', error.message);
                }
            }
            if (i % 500 === 0) console.log(`Progresso: ${i}/${stats.total}...`);
        }
        
        console.log('✅ Importação V6 Finalizada! Dados limpos e segregados.');

    } catch (error) {
        console.error('❌ Erro Fatal:', error.message);
    }
}

runImport();
