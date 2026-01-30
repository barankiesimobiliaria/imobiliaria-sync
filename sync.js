require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY,
    { auth: { persistSession: false } }
);

function lerValor(v) {
    if (!v) return 0;
    if (typeof v === 'object') return parseFloat(v['#text'] || 0);
    const n = parseFloat(v);
    return isNaN(n) ? 0 : n;
}

function lerTexto(v) {
    if (!v) return '';
    if (typeof v === 'object') return v['#text'] || '';
    return String(v).trim();
}

function gerarHash(obj) {
    return crypto
        .createHash('md5')
        .update(JSON.stringify(obj))
        .digest('hex');
}

async function runImport() {
    const agora = new Date().toISOString();

    const stats = {
        totalXml: 0,
        novos: 0,
        atualizados: 0,
        desativados: 0,
        erro: false,
        mensagemErro: null
    };

    try {
        console.log('🔄 Resetando seen_today...');
        await supabase
            .from('cache_xml_externo')
            .update({ seen_today: false })
            .eq('xml_provider', PROVIDER_NAME);

        console.log('📦 Carregando dados existentes...');
        const { data: existentes } = await supabase
            .from('cache_xml_externo')
            .select('listing_id, data_hash')
            .eq('xml_provider', PROVIDER_NAME);

        const mapa = new Map();
        (existentes || []).forEach(e => {
            mapa.set(e.listing_id, e.data_hash);
        });

        console.log('📥 Baixando XML...');
        const xml = await axios.get(XML_URL, { timeout: 120000 });
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
        const json = parser.parse(xml.data);

        const raw = json?.ListingDataFeed?.Listings?.Listing;
        if (!raw) throw new Error('XML inválido');

        const listings = Array.isArray(raw) ? raw : [raw];
        stats.totalXml = listings.length;

        console.log(`📄 ${stats.totalXml} imóveis encontrados`);

        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = [];

            for (const item of batch) {
                const listing_id = lerTexto(item.ListingID);
                if (!listing_id) continue;

                const details = item.Details || {};
                const location = item.Location || {};

                const dataBase = {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    tipo: lerTexto(details.PropertyType),
                    finalidade: lerTexto(item.TransactionType),
                    status: 'ativo',
                    endereco: lerTexto(location.Address),
                    cidade: lerTexto(location.City)?.toUpperCase() || null,
                    bairro: lerTexto(location.Neighborhood),
                    uf: lerTexto(location.State) || 'PR',
                    quartos: lerValor(details.Bedrooms),
                    suites: lerValor(details.Suites),
                    banheiros: lerValor(details.Bathrooms),
                    vagas_garagem: lerValor(details.Garage),
                    area_total: lerValor(details.LotArea),
                    area_util: lerValor(details.LivingArea),
                    valor_venda: lerValor(details.ListPrice),
                    valor_aluguel: lerValor(details.RentalPrice),
                    valor_condominio: lerValor(details.PropertyAdministrationFee),
                    descricao: lerTexto(details.Description),
                    fotos_urls: [],
                    seen_today: true,
                    last_sync: agora,
                    xml_provider: PROVIDER_NAME
                };

                const hashNovo = gerarHash(dataBase);
                const hashAntigo = mapa.get(listing_id);

                if (!hashAntigo) {
                    stats.novos++;
                    dataBase.data_hash = hashNovo;
                    dataBase.data_ultima_alteracao = agora;
                } else if (hashAntigo !== hashNovo) {
                    stats.atualizados++;
                    dataBase.data_hash = hashNovo;
                    dataBase.data_ultima_alteracao = agora;
                } else {
                    dataBase.data_hash = hashAntigo;
                }

                upsertData.push(dataBase);
            }

            if (upsertData.length) {
                await supabase
                    .from('cache_xml_externo')
                    .upsert(upsertData, { onConflict: 'listing_id' });
            }

            console.log(`✔ ${Math.min(i + BATCH_SIZE, listings.length)}/${listings.length}`);
        }

        console.log('🗑️ Inativando removidos...');
        const { data: des } = await supabase
            .from('cache_xml_externo')
            .update({ status: 'inativo' })
            .match({ xml_provider: PROVIDER_NAME, seen_today: false, status: 'ativo' })
            .select();

        stats.desativados = des ? des.length : 0;

        console.log('✅ Importação concluída:', stats);

    } catch (err) {
        console.error('💥 ERRO:', err.message);
        stats.erro = true;
        stats.mensagemErro = err.message;
    }
}

runImport();
