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

/* ================= HELPERS ================= */

function lerValor(v) {
  if (v == null) return 0;
  if (typeof v === 'object') return parseFloat(v['#text'] || 0);
  const n = parseFloat(v);
  return isNaN(n) ? 0 : n;
}

function lerTexto(v) {
  if (!v) return '';
  if (typeof v === 'object') return v['#text'] || '';
  return String(v).trim();
}

function lerFeatures(node) {
  if (!node?.Feature) return [];
  return (Array.isArray(node.Feature) ? node.Feature : [node.Feature])
    .map(f => lerTexto(f))
    .filter(Boolean);
}

function gerarHash(d) {
  return crypto.createHash('md5').update(
    JSON.stringify({
      titulo: d.titulo,
      tipo: d.tipo,
      finalidade: d.finalidade,
      cidade: d.cidade,
      bairro: d.bairro,
      endereco: d.endereco,
      quartos: d.quartos,
      suites: d.suites,
      banheiros: d.banheiros,
      vagas: d.vagas_garagem,
      area_total: d.area_total,
      area_util: d.area_util,
      venda: d.valor_venda,
      aluguel: d.valor_aluguel,
      condominio: d.valor_condominio,
      descricao: d.descricao,
      fotos: (d.fotos_urls || []).slice().sort()
    })
  ).digest('hex');
}

/* ================= MAIN ================= */

async function runImport() {
  console.log('🚀 Iniciando sincronização');

  const stats = {
    totalXml: 0,
    novos: 0,
    atualizados: 0,
    desativados: 0,
    erro: false,
    mensagemErro: null
  };

  try {
    // 1️⃣ Buscar hashes existentes
    const { data: existentes } = await supabase
      .from('cache_xml_externo')
      .select('listing_id, data_hash')
      .eq('xml_provider', PROVIDER_NAME);

    const mapaHashes = new Map(
      (existentes || []).map(e => [e.listing_id, e.data_hash])
    );

    // 2️⃣ Reset seen_today
    await supabase
      .from('cache_xml_externo')
      .update({ seen_today: false })
      .eq('xml_provider', PROVIDER_NAME);

    // 3️⃣ Baixar XML
    const xml = await axios.get(XML_URL, { timeout: 60000 });
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
    const json = parser.parse(xml.data);

    const raw = json?.ListingDataFeed?.Listings?.Listing;
    if (!raw) throw new Error('XML inválido');

    const listings = Array.isArray(raw) ? raw : [raw];
    stats.totalXml = listings.length;

    const agora = new Date().toISOString();

    // 4️⃣ Processar SEM CONDIÇÃO
    for (let i = 0; i < listings.length; i += BATCH_SIZE) {
      const batch = listings.slice(i, i + BATCH_SIZE);
      const upsertData = [];

      for (const item of batch) {
        const listing_id = lerTexto(item.ListingID);
        if (!listing_id) continue;

        const details = item.Details || {};
        const location = item.Location || {};
        const transacao = lerTexto(item.TransactionType).toLowerCase();

        const dados = {
          listing_id,
          xml_provider: PROVIDER_NAME,
          titulo: lerTexto(item.Title),
          tipo: lerTexto(details.PropertyType),
          finalidade: lerTexto(item.TransactionType),
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
          valor_venda: transacao.includes('sale') ? lerValor(details.ListPrice) : 0,
          valor_aluguel: transacao.includes('rent') ? lerValor(details.RentalPrice) : 0,
          valor_condominio: lerValor(details.PropertyAdministrationFee),
          iptu: lerValor(details.YearlyTax) || lerValor(details.MonthlyTax),
          descricao: lerTexto(details.Description),
          diferenciais: lerFeatures(details.Features),
          fotos_urls: [],
          seen_today: true,
          last_sync: agora
        };

        const hashNovo = gerarHash(dados);
        const hashAntigo = mapaHashes.get(listing_id);

        dados.data_hash = hashNovo;

        if (!hashAntigo) {
          stats.novos++;
          dados.data_ultima_alteracao = agora;
        } else if (hashAntigo !== hashNovo) {
          stats.atualizados++;
          dados.data_ultima_alteracao = agora;
        }

        upsertData.push(dados);
      }

      if (upsertData.length) {
        await supabase
          .from('cache_xml_externo')
          .upsert(upsertData, {
            onConflict: 'listing_id,xml_provider'
          });
      }

      console.log(`✔️ Processado ${Math.min(i + BATCH_SIZE, listings.length)}/${listings.length}`);
    }

    // 5️⃣ Inativar removidos
    const { data: desat } = await supabase
      .from('cache_xml_externo')
      .update({ status: 'inativo' })
      .eq('xml_provider', PROVIDER_NAME)
      .eq('seen_today', false)
      .select();

    stats.desativados = desat?.length || 0;

    console.log('✅ Concluído:', stats);

  } catch (e) {
    console.error('💥 ERRO:', e.message);
  }
}

runImport();
