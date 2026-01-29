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

function normalizarParaHash(valor) {
    if (valor === null || valor === undefined) return '';
    if (typeof valor === 'number') return valor.toString();
    if (typeof valor === 'string') return valor.trim();
    if (Array.isArray(valor)) return JSON.stringify(valor.sort());
    return String(valor);
}

function gerarHash(dados) {
    const obj = {
        titulo: normalizarParaHash(dados.titulo),
        tipo: normalizarParaHash(dados.tipo),
        finalidade: normalizarParaHash(dados.finalidade),
        endereco: normalizarParaHash(dados.endereco),
        cidade: normalizarParaHash(dados.cidade),
        bairro: normalizarParaHash(dados.bairro),
        quartos: normalizarParaHash(dados.quartos),
        suites: normalizarParaHash(dados.suites),
        banheiros: normalizarParaHash(dados.banheiros),
        vagas_garagem: normalizarParaHash(dados.vagas_garagem),
        area_total: normalizarParaHash(parseFloat(dados.area_total) || 0),
        area_util: normalizarParaHash(parseFloat(dados.area_util) || 0),
        valor_venda: normalizarParaHash(parseFloat(dados.valor_venda) || 0),
        valor_aluguel: normalizarParaHash(parseFloat(dados.valor_aluguel) || 0),
        valor_condominio: normalizarParaHash(parseFloat(dados.valor_condominio) || 0),
        descricao: normalizarParaHash(dados.descricao),
        fotos: normalizarParaHash(dados.fotos_urls || [])
    };
    return crypto.createHash('md5').update(JSON.stringify(obj)).digest('hex');
}

async function registrarLog(stats) {
    try {
        const logData = {
            data_execucao: new Date().toISOString(),
            status: stats.erro ? 'erro' : 'sucesso',
            total_xml: stats.totalXml,
            novos: stats.novos,
            atualizados: stats.atualizados,
            removidos: stats.desativados,
            mensagem_erro: stats.mensagemErro || null
        };
        
        // Tenta incluir sem_alteracao se a coluna existir
        if (stats.semAlteracao !== undefined) {
            logData.sem_alteracao = stats.semAlteracao;
        }
        
        const { error } = await supabase.from('import_logs').insert(logData);
        
        if (error) {
            console.error('⚠️ Erro ao salvar log:', error.message);
        } else {
            console.log('📝 Log de importação registrado!');
        }
    } catch (err) {
        console.error('⚠️ Falha ao registrar log:', err.message);
    }
}

// ============================================
// BUSCA TODOS OS REGISTROS (COM PAGINAÇÃO)
// ============================================
async function buscarTodosExistentes() {
    const todos = [];
    let offset = 0;
    const limite = 1000;
    
    while (true) {
        const { data, error } = await supabase
            .from('cache_xml_externo')
            .select('listing_id, titulo, tipo, finalidade, endereco, cidade, bairro, quartos, suites, banheiros, vagas_garagem, area_total, area_util, valor_venda, valor_aluguel, valor_condominio, descricao, fotos_urls')
            .eq('xml_provider', PROVIDER_NAME)
            .range(offset, offset + limite - 1);
        
        if (error) {
            throw new Error(`Erro ao buscar existentes: ${error.message}`);
        }
        
        if (!data || data.length === 0) break;
        
        todos.push(...data);
        
        if (data.length < limite) break;
        
        offset += limite;
    }
    
    return todos;
}

async function runImport() {
    console.log('');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 INICIANDO SINCRONIZAÇÃO XML');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('');
    
    let stats = { 
        totalXml: 0, 
        novos: 0, 
        atualizados: 0, 
        semAlteracao: 0,
        desativados: 0, 
        erros: 0,
        erro: false,
        mensagemErro: null
    };

    try {
        // ============================================
        // PASSO 1: Buscar TODOS os dados existentes
        // ============================================
        console.log('📦 Passo 1: Buscando TODOS os dados existentes...');
        
        const existentes = await buscarTodosExistentes();
        
        const dadosExistentes = new Map();
        existentes.forEach(e => {
            dadosExistentes.set(e.listing_id, {
                hash: gerarHash(e),
                dados: e
            });
        });
        
        console.log(`   ✓ ${dadosExistentes.size} imóveis encontrados no banco`);

        // ============================================
        // PASSO 2: Resetar flags
        // ============================================
        console.log('');
        console.log('🔄 Passo 2: Resetando flags seen_today...');
        
        await supabase
            .from('cache_xml_externo')
            .update({ seen_today: false })
            .eq('xml_provider', PROVIDER_NAME);
        
        console.log('   ✓ Flags resetadas');

        // ============================================
        // PASSO 3: Baixar e parsear XML
        // ============================================
        console.log('');
        console.log('📥 Passo 3: Baixando XML...');
        
        const response = await axios.get(XML_URL, { 
            timeout: 120000,
            responseType: 'text',
            maxContentLength: 100 * 1024 * 1024
        });
        
        console.log('   ✓ XML baixado, parseando...');
        
        const parser = new XMLParser({ 
            ignoreAttributes: false, 
            attributeNamePrefix: "@_",
            parseTagValue: false
        });
        const jsonData = parser.parse(response.data);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) {
            throw new Error("XML vazio ou estrutura inválida");
        }
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;
        
        console.log(`   ✓ ${stats.totalXml} imóveis no XML`);

        // ============================================
        // PASSO 4: Processar imóveis
        // ============================================
        console.log('');
        console.log('⚙️ Passo 4: Processando imóveis...');
        
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
                        if ((m['@_primary'] === 'true' || m['@_primary'] === true) && !capa) {
                            capa = url;
                        } else {
                            fotos.push(url);
                        }
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

                const existente = dadosExistentes.get(listing_id);
                
                if (!existente) {
                    stats.novos++;
                    upsertData.push(dadosImovel);
                } else {
                    const hashNovo = gerarHash(dadosImovel);
                    
                    if (hashNovo !== existente.hash) {
                        stats.atualizados++;
                        upsertData.push(dadosImovel);
                    } else {
                        stats.semAlteracao++;
                        upsertData.push({
                            listing_id,
                            seen_today: true,
                            last_sync: new Date().toISOString(),
                            status: 'ativo'
                        });
                    }
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase
                    .from('cache_xml_externo')
                    .upsert(upsertData, { onConflict: 'listing_id' });
                    
                if (error) {
                    stats.erros += upsertData.length;
                    console.error(`   ❌ Erro no batch: ${error.message}`);
                }
            }
            
            const progresso = Math.min(i + BATCH_SIZE, listings.length);
            console.log(`   📊 ${progresso}/${listings.length}`);
        }

        // ============================================
        // PASSO 5: Inativar removidos
        // ============================================
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
        
        console.log(`   ✓ ${stats.desativados} removidos`);
        
        // ============================================
        // RESULTADO
        // ============================================
        console.log('');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('✅ SINCRONIZAÇÃO CONCLUÍDA!');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`   📄 Total XML:       ${stats.totalXml}`);
        console.log(`   🆕 Novos:           ${stats.novos}`);
        console.log(`   🔄 Atualizados:     ${stats.atualizados}`);
        console.log(`   ✨ Sem alteração:   ${stats.semAlteracao}`);
        console.log(`   ❌ Removidos:       ${stats.desativados}`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

        // Validação
        const total = stats.novos + stats.atualizados + stats.semAlteracao;
        if (total !== stats.totalXml) {
            console.warn(`⚠️ Diferença: processados=${total}, XML=${stats.totalXml}`);
        }

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
