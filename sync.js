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

// ============================================
// FUNÇÕES AUXILIARES
// ============================================

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

// Gera hash MD5 dos dados principais
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

// Busca TODOS os listing_id e hash existentes (com paginação)
async function buscarHashesExistentes() {
    console.log('   Buscando hashes existentes...');
    const mapa = new Map();
    let offset = 0;
    const limite = 1000;
    let totalBuscado = 0;
    
    while (true) {
        const { data, error } = await supabase
            .from('cache_xml_externo')
            .select('listing_id, data_hash')
            .eq('xml_provider', PROVIDER_NAME)
            .order('listing_id')
            .range(offset, offset + limite - 1);
        
        if (error) {
            console.error(`   Erro na busca offset ${offset}:`, error.message);
            break;
        }
        
        if (!data || data.length === 0) {
            console.log(`   Fim da busca no offset ${offset}`);
            break;
        }
        
        data.forEach(item => {
            mapa.set(item.listing_id, item.data_hash || '');
        });
        
        totalBuscado += data.length;
        console.log(`   ... buscados ${totalBuscado} registros`);
        
        if (data.length < limite) break;
        offset += limite;
    }
    
    return mapa;
}

async function registrarLog(stats) {
    try {
        await supabase.from('import_logs').insert({
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

// ============================================
// FUNÇÃO PRINCIPAL
// ============================================

async function runImport() {
    console.log('');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 SINCRONIZAÇÃO XML - v3.0');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    let stats = { 
        totalXml: 0, 
        novos: 0, 
        atualizados: 0, 
        semAlteracao: 0,
        desativados: 0,
        erro: false,
        mensagemErro: null
    };

    try {
        // PASSO 1: Buscar hashes existentes
        console.log('');
        console.log('📦 Passo 1: Buscando dados existentes...');
        const hashesExistentes = await buscarHashesExistentes();
        console.log(`   ✅ ${hashesExistentes.size} imóveis no banco`);

        // PASSO 2: Resetar flags
        console.log('');
        console.log('🔄 Passo 2: Resetando flags...');
        const { error: errReset } = await supabase
            .from('cache_xml_externo')
            .update({ seen_today: false })
            .eq('xml_provider', PROVIDER_NAME);
        
        if (errReset) console.warn('   ⚠️ Aviso reset:', errReset.message);
        else console.log('   ✅ Flags resetadas');

        // PASSO 3: Baixar XML
        console.log('');
        console.log('📥 Passo 3: Baixando XML...');
        const response = await axios.get(XML_URL, { 
            timeout: 120000,
            responseType: 'text'
        });
        
        const parser = new XMLParser({ 
            ignoreAttributes: false, 
            attributeNamePrefix: "@_"
        });
        const jsonData = parser.parse(response.data);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou inválido");
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;
        console.log(`   ✅ ${stats.totalXml} imóveis no XML`);

        // PASSO 4: Processar
        console.log('');
        console.log('⚙️ Passo 4: Processando...');
        
        const idsProcessados = new Set();
        
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

                let mediaItems = item.Media?.Item 
                    ? (Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item]) 
                    : [];
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
                    last_sync: new Date().toISOString(),
                    xml_provider: PROVIDER_NAME
                };

                // Calcular hash dos dados novos
                const hashNovo = gerarHash(dadosImovel);
                dadosImovel.data_hash = hashNovo;

                // Verificar se existe e se mudou
                const hashAntigo = hashesExistentes.get(listing_id);
                
                if (hashAntigo === undefined) {
                    // NOVO - não existe no banco
                    stats.novos++;
                    upsertData.push(dadosImovel);
                } else if (hashAntigo !== hashNovo) {
                    // ATUALIZADO - hash diferente
                    stats.atualizados++;
                    upsertData.push(dadosImovel);
                } else {
                    // SEM ALTERAÇÃO - só atualiza flags
                    stats.semAlteracao++;
                    upsertData.push({
                        listing_id,
                        seen_today: true,
                        last_sync: new Date().toISOString(),
                        status: 'ativo'
                    });
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase
                    .from('cache_xml_externo')
                    .upsert(upsertData, { onConflict: 'listing_id' });
                    
                if (error) {
                    console.error(`   ❌ Erro batch: ${error.message}`);
                }
            }
            
            if ((i + BATCH_SIZE) % 200 === 0 || i + BATCH_SIZE >= listings.length) {
                console.log(`   📊 ${Math.min(i + BATCH_SIZE, listings.length)}/${listings.length}`);
            }
        }

        // PASSO 5: Inativar removidos
        console.log('');
        console.log('🗑️ Passo 5: Inativando removidos...');
        
        const { data: paraDesativar } = await supabase
            .from('cache_xml_externo')
            .select('listing_id')
            .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });
        
        if (paraDesativar && paraDesativar.length > 0) {
            await supabase
                .from('cache_xml_externo')
                .update({ status: 'inativo' })
                .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' });
            stats.desativados = paraDesativar.length;
        }
        
        // RESULTADO
        console.log('');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('✅ SINCRONIZAÇÃO CONCLUÍDA!');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`   📄 Total XML:       ${stats.totalXml}`);
        console.log(`   🆕 Novos:           ${stats.novos}`);
        console.log(`   🔄 Atualizados:     ${stats.atualizados}`);
        console.log(`   ✨ Sem alteração:   ${stats.semAlteracao}`);
        console.log(`   ❌ Removidos:       ${stats.desativados}`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        
        // Validação
        const totalProcessado = stats.novos + stats.atualizados + stats.semAlteracao;
        console.log(`   🔍 Validação: ${totalProcessado}/${stats.totalXml}`);
        
        if (totalProcessado !== stats.totalXml) {
            console.warn(`   ⚠️ DIFERENÇA DE ${stats.totalXml - totalProcessado} IMÓVEIS!`);
        }

        await registrarLog(stats);

    } catch (error) {
        console.error('');
        console.error('💥 ERRO FATAL:', error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        await registrarLog(stats);
        process.exit(1);
    }
}

runImport();
