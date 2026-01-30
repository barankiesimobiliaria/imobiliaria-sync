require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error('❌ SUPABASE_URL ou SUPABASE_KEY não configuradas');
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
  auth: { persistSession: false }
});

/* ================= HELPERS ================= */

function lerValor(campo) {
  if (campo === undefined || campo === null) return 0;
  if (typeof campo === 'object') return campo['#text'] ? parseFloat(campo['#text']) : 0;
  const v = parseFloat(campo);
  return isNaN(v) ? 0 : v;
}

function lerTexto(campo) {
  if (!campo) return '';
  if (typeof campo === 'object') return campo['#text'] || '';
  return String(campo).trim();
}

function lerFeatures(node) {
  if (!node || !node.Feature) return [];
  const lista = Array.isArray(node.Feature) ? node.Feature : [node.Feature];
  return lista.map(f => lerTexto(f)).filter(Boolean);
}

function gerarHash(d) {
  const fotosOrdenadas = (d.fotos_urls || []).slice().sort();

  const base = [
    d.titulo || '',
    d.tipo || '',
    d.finalidade || '',
    d.cidade || '',
    d.bairro || '',
    d.endereco || '',
    d.quartos || 0,
    d.suites || 0,
    d.banheiros || 0,
    d.vagas_garagem || 0,
    d.area_total || 0,
    d.area_util || 0,
    d.valor_venda || 0,
    d.valor_aluguel || 0,
    d.valor_condominio || 0,
    d.descricao || '',
    JSON.stringify(fotosOrdenadas)
  ].join('|');

  return crypto.createHash('md5').update(base).digest('hex');
}

/* ================= DB ================= */

async function buscarHashesExistentes() {
  const mapa = new Map();
  let offset = 0;
  const limite = 1000;

  while (true) {
    const { data, error } = await supabase
      .from('cache_xml_externo')
      .select('listing_id, data_hash')
      .eq('xml_provider', PROVIDER_NAME)
      .order('listing_id')
      .range(offset, offset + limite - 1);

    if (error || !data || data.length === 0) break;

    data.forEach(r => mapa.set(r.listing_id, r.data_hash || ''));
    if (data.length < limite) break;
    offset += limite;
  }

  return mapa;
}

async function registrarLog(stats) {
  await supabase.from('import_logs').insert({
    status: stats.erro === true ? 'erro' : 'sucesso',
    total_xml: stats.totalXml,
    novos: stats.novos,
    atualizados: stats.atualizados,
    removidos: stats.desativados,
    sem_alteracao: stats.semAlteracao,
    mensagem_erro: stats.mensagemErro || null
  });
}

/* ================= MAIN ================= */

async function runImport() {
  const stats = {
    totalXml: 0,
    novos: 0,
    atualizados: 0,
    semAlteracao: 0,
    desativados: 0,
    erro: false,
    mensagemErro: null
  };

  try {
    const hashesExistentes = await buscarHashesExistentes();

    await supabase
      .from('cache_xml_externo')
      .update({ seen_today: false })
      .eq('xml_provider', PROVIDER_NAME);

    const response = await axios.get(XML_URL, { timeout: 120000 });
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
    const json = parser.parse(response.data);

    const listingsRaw = json?.ListingDataFeed?.Listings?.Listing;
    if (!listingsRaw) throw new Error('XML inválido');

    const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
    stats.totalXml = listings.length;

    const idsProcessados = new Set();
    const agora = new Date().toISOString();

    for (let i = 0; i < listings.length; i += BATCH_SIZE) {
      const batch = listings.slice(i, i + BATCH_SIZE);
      const upsertData = [];

      for (const item of batch) {
        const listing_id = lerTexto(item.ListingID);
        if (!listing_id || idsProcessados.has(listing_id)) continue;
        idsProcessados.add(listing_id);

        const details = item.Details || {};
        const location = item.Location || {};

        const transacaoRaw = lerTexto(item.TransactionType).toLowerCase();
        const venda = transacaoRaw.includes('sale');
        const aluguel = transacaoRaw.includes('rent');

        const fotos = [];
        const media = item.Media?.Item
          ? Array.isArray(item.Media.Item) ? item.Media.Item : [item.Media.Item]
          : [];

        media.forEach(m => {
          const url = lerTexto(m);
          if (url.startsWith('http')) fotos.push(url);
        });

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
          valor_venda: venda ? lerValor(details.ListPrice) : 0,
          valor_aluguel: aluguel ? lerValor(details.RentalPrice) : 0,
          valor_condominio: lerValor(details.PropertyAdministrationFee),
          iptu: lerValor(details.YearlyTax) || lerValor(details.MonthlyTax),
          descricao: lerTexto(details.Description),
          diferenciais: lerFeatures(details.Features),
          fotos_urls: fotos.sort(),
          seen_today: true,
          last_sync: agora
        };

        const hashNovo = gerarHash(dados);
        const hashAntigo = hashesExistentes.get(listing_id);

        dados.data_hash = hashNovo;

        if (hashAntigo === undefined || hashAntigo !== hashNovo) {
          dados.data_ultima_alteracao = agora;
          hashAntigo === undefined ? stats.novos++ : stats.atualizados++;
          upsertData.push(dados);
        } else {
          stats.semAlteracao++;
          upsertData.push({
            listing_id,
            xml_provider: PROVIDER_NAME,
            status: 'ativo',
            seen_today: true,
            last_sync: agora
          });
        }
      }

      if (upsertData.length) {
        await supabase
          .from('cache_xml_externo')
          .upsert(upsertData, {
            onConflict: 'listing_id,xml_provider'
          });
      }
    }

    const { data: inativar } = await supabase
      .from('cache_xml_externo')
      .select('listing_id')
      .eq('xml_provider', PROVIDER_NAME)
      .eq('seen_today', false);

    if (inativar?.length) {
      await supabase
        .from('cache_xml_externo')
        .update({ status: 'inativo' })
        .eq('xml_provider', PROVIDER_NAME)
        .eq('seen_today', false);

      stats.desativados = inativar.length;
    }

    await registrarLog(stats);

  } catch (err) {
    stats.erro = true;
    stats.mensagemErro = err.message;
    await registrarLog(stats);
    throw err;
  }
}

runImport();
