require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const axios = require("axios");
const { XMLParser } = require("fast-xml-parser");
const crypto = require("crypto");

const XML_URL = "https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9";
const PROVIDER_NAME = "RedeUrbana";
const BATCH_SIZE = 50;
const TABELA_CACHE = "cache_xml_externo";
const TABELA_LOGS = "import_logs";

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
    // Se for um objeto (como <Price currency="BRL">100</Price>), tenta pegar o texto
    if (typeof campo === "object") {
        const valStr = campo["#text"] || campo["text"] || "";
        const val = parseFloat(valStr);
        return isNaN(val) ? 0 : val;
    }
    const val = parseFloat(campo);
    return isNaN(val) ? 0 : val;
}

function lerTexto(campo) {
    if (!campo) return "";
    let texto = "";
    if (typeof campo === "object") {
        texto = campo["#text"] || campo["text"] || "";
    } else {
        texto = String(campo);
    }
    // Remove caracteres não imprimíveis e espaços extras, mas preserva acentuação comum em PT-BR
    return texto.replace(/\s+/g, " ").trim();
}

function lerFeatures(featuresNode) {
    if (!featuresNode || !featuresNode.Feature) return [];
    const feat = featuresNode.Feature;
    const lista = Array.isArray(feat) ? feat : [feat];
    return lista.map(f => lerTexto(f)).filter(f => f !== "").sort();
}

function gerarHash(d) {
    const normalizarNumero = (n) => {
        const num = parseFloat(n) || 0;
        return num.toFixed(2);
    };

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
        normalizarNumero(d.area_total),
        normalizarNumero(d.area_util),
        normalizarNumero(d.valor_venda),
        normalizarNumero(d.valor_aluguel),
        normalizarNumero(d.valor_condominio),
        // Pegamos um pedaço maior da descrição para o hash ser mais preciso
        lerTexto(d.descricao).substring(0, 1000),
        (d.fotos_urls || []).sort().join(","),
        (d.diferenciais || []).sort().join(",")
    ].join("|").toLowerCase();
    
    return crypto.createHash("md5").update(str).digest("hex");
}

async function buscarDadosExistentes() {
    console.log("   Buscando TODOS os imóveis do banco (Sincronia Global)...");
    const mapa = new Map();
    let offset = 0;
    const limite = 1000;
    
    while (true) {
        const { data, error } = await supabase
            .from(TABELA_CACHE)
            .select("listing_id, data_hash, xml_provider, status") 
            .range(offset, offset + limite - 1);
        
        if (error) throw error;
        if (!data || data.length === 0) break;
        
        data.forEach(item => {
            mapa.set(String(item.listing_id).trim(), {
                hash: item.data_hash || "",
                status: item.status,
                provider: item.xml_provider
            });
        });
        
        if (data.length < limite) break;
        offset += limite;
    }
    console.log(`   ✅ ${mapa.size} registros totais carregados do banco.`);
    return mapa;
}

async function registrarLog(stats) {
    try {
        await supabase.from(TABELA_LOGS).insert({
            data_execucao: new Date().toISOString(),
            status: stats.erro ? "erro" : "sucesso",
            total_xml: stats.totalXml,
            novos: stats.novos,
            atualizados: stats.atualizados,
            removidos: stats.desativados,
            sem_alteracao: stats.semAlteracao,
            mensagem_erro: stats.mensagemErro || null
        });
        console.log("📝 Log de execução registrado.");
    } catch (err) {
        console.error("⚠️ Erro ao salvar log:", err.message);
    }
}

// --- CORE DA SINCRONIZAÇÃO ---

async function runImport() {
    console.log("\n🚀 INICIANDO SINCRONIZAÇÃO GLOBAL CORRIGIDA");
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false, mensagemErro: null };

    try {
        const dadosExistentes = await buscarDadosExistentes();

        console.log(`📥 Baixando XML de: ${XML_URL}`);
        const response = await axios.get(XML_URL, { 
            headers: { 'Cache-Control': 'no-cache' },
            responseType: 'text' // Garante que recebemos como string para o parser
        });
        
        const parser = new XMLParser({ 
            ignoreAttributes: false, 
            attributeNamePrefix: "@_",
            processEntities: true,
            trimValues: true
        });
        const jsonData = parser.parse(response.data);
        
        // Caminho correto no XML do VivaReal/VRSync
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou estrutura 'ListingDataFeed > Listings > Listing' não encontrada");
        
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        const idsNoXml = new Set();
        const agora = new Date().toISOString();
        
        console.log(`🔄 Processando ${listings.length} imóveis do XML...`);
        
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];
            
            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id) continue;
                
                // Evita duplicados dentro do próprio XML
                if (idsNoXml.has(listing_id)) continue;
                idsNoXml.add(listing_id);

                const details = item.Details || {};
                const location = item.Location || {};
                const transacao = lerTexto(item.TransactionType);
                
                let vVenda = lerValor(details.ListPrice);
                let vAluguel = lerValor(details.RentalPrice);
                
                // Processamento de Fotos
                let mediaItems = [];
                if (item.Media && item.Media.Item) {
                    mediaItems = Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item];
                }
                
                let fotos = [];
                let capa = null;
                mediaItems.forEach(m => {
                    const url = lerTexto(m);
                    if (url && url.startsWith("http")) {
                        // Verifica se é a imagem principal
                        const isPrimary = m["@_primary"] === "true" || m["@_primary"] === true;
                        if (isPrimary && !capa) {
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
                    status: "ativo", // REGRA: Se está no XML, deve estar ATIVO
                    endereco: lerTexto(location.Address),
                    cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood),
                    uf: lerTexto(location.State) || "PR",
                    latitude: location.Latitude ? String(location.Latitude) : null,
                    longitude: location.Longitude ? String(location.Longitude) : null,
                    quartos: parseInt(lerValor(details.Bedrooms)) || 0,
                    suites: parseInt(lerValor(details.Suites)) || 0,
                    banheiros: parseInt(lerValor(details.Bathrooms)) || 0,
                    vagas_garagem: parseInt(lerValor(details.Garage)) || 0,
                    area_total: lerValor(details.LotArea) || lerValor(details.LandArea),
                    area_util: lerValor(details.LivingArea) || lerValor(details.ConstructedArea),
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
                
                if (!registroAntigo) {
                    // Imóvel novo no banco
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else if (registroAntigo.hash !== hashNovo || registroAntigo.status !== "ativo") {
                    // Imóvel mudou ou estava inativo e agora está no XML (reativar)
                    stats.atualizados++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    // Nenhuma mudança relevante, apenas atualiza timestamp de visto
                    stats.semAlteracao++;
                    upsertData.push({ 
                        listing_id, 
                        seen_today: true, 
                        last_sync: agora, 
                        status: "ativo" 
                    });
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: "listing_id" });
                if (error) throw new Error(`Erro no upsert: ${error.message}`);
            }
        }

        // --- INATIVAÇÃO DE AUSENTES ---
        console.log("🗑️ Verificando imóveis para inativação (não estão no XML)...");
        const idsParaInativar = [];
        for (const [listing_id, info] of dadosExistentes.entries()) {
            // Se o imóvel está ativo no banco mas NÃO está no conjunto de IDs que acabamos de ler do XML
            if (info.status === "ativo" && !idsNoXml.has(listing_id)) {
                idsParaInativar.push(listing_id);
            }
        }

        if (idsParaInativar.length > 0) {
            console.log(`   Inativando ${idsParaInativar.length} imóveis...`);
            for (let i = 0; i < idsParaInativar.length; i += BATCH_SIZE) {
                const loteIds = idsParaInativar.slice(i, i + BATCH_SIZE);
                const { error } = await supabase.from(TABELA_CACHE)
                    .update({ 
                        status: "inativo", 
                        seen_today: false, 
                        data_ultima_alteracao: agora, 
                        last_sync: agora 
                    })
                    .in("listing_id", loteIds);
                
                if (error) throw new Error(`Erro ao inativar lote: ${error.message}`);
            }
            stats.desativados = idsParaInativar.length;
        } else {
            console.log("   Nenhum imóvel para inativar.");
        }
        
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.log("✅ SINCRONIZAÇÃO CONCLUÍDA COM SUCESSO!");
        console.log(`📊 Total no XML: ${stats.totalXml}`);
        console.log(`✨ Novos/Reativados: ${stats.novos}`);
        console.log(`🔄 Atualizados: ${stats.atualizados}`);
        console.log(`🗑️ Inativados: ${stats.desativados}`);
        console.log(`✔️ Sem alteração: ${stats.semAlteracao}`);
        console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        await registrarLog(stats);

    } catch (error) {
        console.error("💥 ERRO CRÍTICO:", error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        await registrarLog(stats);
        process.exit(1);
    }
}

runImport();
