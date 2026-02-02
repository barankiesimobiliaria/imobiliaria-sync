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

function lerValor(campo) {
    if (campo === undefined || campo === null) return 0;
    if (typeof campo === "object") return campo["#text"] ? parseFloat(campo["#text"]) : 0;
    const val = parseFloat(campo);
    return isNaN(val) ? 0 : val;
}

function lerTexto(campo) {
    if (!campo) return "";
    let texto = "";
    if (typeof campo === "object") {
        texto = campo["#text"] || "";
    } else {
        texto = String(campo);
    }
    return texto.replace(/\s+/g, " ").replace(/[^\x20-\x7E]/g, "").trim();
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
        lerTexto(d.descricao).substring(0, 500).replace(/[^a-zA-Z0-9 ]/g, ""),
        (d.fotos_urls || []).sort().join(","),
        (d.diferenciais || []).sort().join(",")
    ].join("|").toLowerCase();
    
    return crypto.createHash("md5").update(str).digest("hex");
}

async function buscarDadosExistentes() {
    console.log("   Buscando dados existentes...");
    const mapa = new Map();
    let offset = 0;
    const limite = 1000;
    
    while (true) {
        const { data, error } = await supabase
            .from(TABELA_CACHE)
            .select("listing_id, data_hash, xml_provider, status") 
            .eq("xml_provider", PROVIDER_NAME)
            .range(offset, offset + limite - 1);
        
        if (error) {
            console.error(`   Erro na busca:`, error.message);
            throw error;
        }
        
        if (!data || data.length === 0) break;
        
        data.forEach(item => {
            mapa.set(String(item.listing_id).trim(), {
                hash: item.data_hash || "",
                provider: item.xml_provider,
                status: item.status
            });
        });
        
        if (data.length < limite) break;
        offset += limite;
    }
    
    console.log(`   ✅ ${mapa.size} registros carregados do provedor ${PROVIDER_NAME}`);
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
        console.log("📝 Log registrado!");
    } catch (err) {
        console.error("⚠️ Erro ao salvar log:", err.message);
    }
}

async function runImport() {
    console.log("");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("🚀 SINCRONIZAÇÃO XML - V8 (FINAL)");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let stats = { totalXml: 0, novos: 0, atualizados: 0, semAlteracao: 0, desativados: 0, erro: false, mensagemErro: null };

    try {
        const dadosExistentes = await buscarDadosExistentes();

        console.log("📥 Baixando XML...");
        const response = await axios.get(XML_URL);
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        if (!listingsRaw) throw new Error("XML vazio ou inválido");
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        stats.totalXml = listings.length;

        const idsNoXml = new Set();
        const agora = new Date().toISOString();
        
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];
            
            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id || idsNoXml.has(listing_id)) continue;
                idsNoXml.add(listing_id);

                const details = item.Details || {};
                const location = item.Location || {};
                const transacao = lerTexto(item.TransactionType);
                
                let vVenda = 0, vAluguel = 0;
                const pVenda = lerValor(details.ListPrice);
                const pAluguel = lerValor(details.RentalPrice);
                if (transacao === "For Rent") vAluguel = pAluguel || pVenda;
                else if (transacao === "For Sale") vVenda = pVenda;
                else { vVenda = pVenda; vAluguel = pAluguel; }

                let mediaItems = item.Media?.Item ? (Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item]) : [];
                let fotos = [];
                let capa = null;
                mediaItems.forEach(m => {
                    const url = lerTexto(m);
                    if (url && url.startsWith("http")) {
                        if ((m["@_primary"] === "true" || m["@_primary"] === true) && !capa) capa = url;
                        else fotos.push(url);
                    }
                });
                if (capa) fotos.unshift(capa);

                const dadosImovel = {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    tipo: lerTexto(details.PropertyType),
                    finalidade: transacao,
                    status: "ativo",
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
                
                if (!registroAntigo) {
                    stats.novos++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else if (registroAntigo.hash !== hashNovo || registroAntigo.status !== "ativo") {
                    stats.atualizados++;
                    dadosImovel.data_ultima_alteracao = agora;
                    upsertData.push(dadosImovel);
                } else {
                    stats.semAlteracao++;
                    upsertData.push({ listing_id, seen_today: true, last_sync: agora, status: "ativo" });
                }
            }

            if (upsertData.length > 0) {
                const { error } = await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: "listing_id" });
                if (error) throw new Error(`Erro no upsert: ${error.message}`);
            }
        }

        console.log("🗑️ Processando inativações...");
        // Identificamos quem está ativo no banco mas NÃO está no XML atual
        const idsParaInativar = [];
        for (const [listing_id, info] of dadosExistentes.entries()) {
            if (info.status === "ativo" && !idsNoXml.has(listing_id)) {
                idsParaInativar.push(listing_id);
            }
        }

        if (idsParaInativar.length > 0) {
            console.log(`   Inativando ${idsParaInativar.length} imóveis ausentes no XML...`);
            
            // Processamos em lotes para evitar limites de URL/Payload do Supabase
            for (let i = 0; i < idsParaInativar.length; i += BATCH_SIZE) {
                const loteIds = idsParaInativar.slice(i, i + BATCH_SIZE);
                const { error } = await supabase
                    .from(TABELA_CACHE)
                    .update({ status: "inativo", seen_today: false, data_ultima_alteracao: agora })
                    .in("listing_id", loteIds)
                    .eq("xml_provider", PROVIDER_NAME);
                
                if (error) throw new Error(`Erro ao inativar lote: ${error.message}`);
            }
            stats.desativados = idsParaInativar.length;
        } else {
            console.log("   Nenhum imóvel para inativar.");
        }
        
        console.log("✅ CONCLUÍDO!");
        await registrarLog(stats);

    } catch (error) {
        console.error("💥 ERRO:", error.message);
        stats.erro = true;
        stats.mensagemErro = error.message;
        await registrarLog(stats);
        process.exit(1);
    }
}

runImport();
