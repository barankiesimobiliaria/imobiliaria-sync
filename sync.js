require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';

// Verifica se as chaves existem
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    console.error("❌ ERRO: Configure o .env com SUPABASE_URL e SUPABASE_KEY");
    process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
    auth: { persistSession: false }
});

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
    console.log(`[${new Date().toISOString()}] 🚀 Iniciando Importação V11 (Completa e Segura)...`);
    let stats = { total: 0, processados: 0, erros: 0, ignorados: 0, desativados: 0 };

    try {
        // --- PASSO 0: RESETAR A FLAG DO DIA ---
        // Isso garante que saberemos quem sumiu do XML depois
        console.log('0. Preparando banco de dados...');
        const { error: resetError } = await supabase
            .from('cache_xml_externo')
            .update({ seen_today: false })
            .neq('id', 0); // Pega todos os registros
            
        if (resetError) console.warn(`⚠️ Aviso no reset (pode ser a primeira execução): ${resetError.message}`);

        // --- PASSO 1: BAIXAR XML ---
        console.log('1. Baixando XML...');
        const response = await axios.get(XML_URL, { responseType: 'text' });
        
        console.log('2. Lendo estrutura...');
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        
        if (!jsonData['ListingDataFeed'] || !jsonData['ListingDataFeed']['Listings']) {
            throw new Error("Estrutura do XML inválida.");
        }

        const listingsRaw = jsonData['ListingDataFeed']['Listings']['Listing'];
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.total = listings.length;
        console.log(`📊 Total imóveis no XML: ${stats.total}`);

        // --- PASSO 2: PROCESSAR EM LOTES ---
        const BATCH_SIZE = 50;
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID)?.trim();
                
                if (!listing_id) {
                    stats.ignorados++;
                    continue;
                }

                try {
                    const details = item.Details || {};
                    const location = item.Location || {};
                    const transacao = lerTexto(item.TransactionType); 
                    const tipoImovel = lerTexto(details.PropertyType);

                    // PREÇOS
                    let vVenda = 0, vAluguel = 0;
                    const rawListPrice = lerValor(details.ListPrice);
                    const rawRentalPrice = lerValor(details.RentalPrice);

                    if (transacao === 'For Rent') vAluguel = rawRentalPrice > 0 ? rawRentalPrice : rawListPrice;
                    else if (transacao === 'For Sale') vVenda = rawListPrice;
                    else { vVenda = rawListPrice; vAluguel = rawRentalPrice; }

                    // FOTOS
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

                    // VALIDADORES
                    let banheiros = parseInt(lerValor(details.Bathrooms)) || 0;
                    let vagas = parseInt(lerValor(details.Garage)) || 0;
                    if (tipoImovel.includes('Residential')) {
                        if (banheiros > 10) banheiros = 10;
                        if (vagas > 20) vagas = 20;
                    }

                    // --- MONTAGEM DO OBJETO ---
                    upsertData.push({
                        listing_id: listing_id,
                        titulo: lerTexto(item.Title),
                        tipo: tipoImovel,
                        finalidade: transacao,
                        status: 'ativo', // Força ativo pois está no XML
                        
                        // ✅ AQUI ESTÁ O ENDEREÇO NOVO
                        endereco: lerTexto(location.Address), 
                        
                        cidade: location.City ? lerTexto(location.City).toUpperCase() : null,
                        bairro: lerTexto(location.Neighborhood),
                        uf: lerTexto(location.State) || 'PR',
                        
                        latitude: location.Latitude ? lerTexto(location.Latitude) : null,
                        longitude: location.Longitude ? lerTexto(location.Longitude) : null,
                        
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
                        
                        seen_today: true, // Marca como visto hoje
                        last_sync: new Date(),
                        xml_provider: 'RedeUrbana'
                    });

                } catch (e) {
                    console.error(`❌ Erro parsing item ${listing_id}:`, e.message);
                    stats.erros++;
                }
            }

            if (upsertData.length > 0) {
                // ✅ SALVA NO BANCO (ATUALIZA SE EXISTIR)
                const { error } = await supabase
                    .from('cache_xml_externo')
                    .upsert(upsertData, { 
                        onConflict: 'listing_id',
                        ignoreDuplicates: false // <--- IMPORTANTE: FALSE para atualizar preços
                    });

                if (error) {
                    console.error('❌ Erro Supabase:', error.message);
                    stats.erros += upsertData.length;
                } else {
                    stats.processados += upsertData.length;
                }
            }
            
            if (i % 500 === 0) console.log(`📈 Processado: ${i}/${stats.total}`);
        }

        // --- PASSO 3: LIMPEZA (QUEM SUMIU VIRA INATIVO) ---
        console.log('3. Atualizando status dos removidos...');
        
        const { data: desativados, error: deleteError } = await supabase
            .from('cache_xml_externo')
            .update({ status: 'inativo' }) 
            .eq('seen_today', false)      // Quem NÃO foi visto hoje
            .neq('status', 'inativo')     // E que ainda estava ativo
            .select();

        if (deleteError) {
            console.error("Erro na limpeza:", deleteError.message);
        } else {
            stats.desativados = desativados ? desativados.length : 0;
            console.log(`🗑️ Imóveis desativados: ${stats.desativados}`);
        }
        
        console.log('🎉 SUCESSO! Importação finalizada.');
        console.log(`📊 Relatório: ${stats.processados} ativos/atualizados, ${stats.desativados} removidos.`);
        
    } catch (error) { 
        console.error('💥 Erro Fatal:', error.message); 
    }
}

runImport();
