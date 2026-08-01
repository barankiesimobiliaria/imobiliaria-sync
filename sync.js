/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 9.4 (Substituição Total e Hashing Robusto)
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

// ═══════════════════════════════════════════
// CONFIGURAÇÕES
// ═══════════════════════════════════════════
const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;
const TABELA_CACHE = 'cache_xml_externo';
const TABELA_LOGS = 'import_logs';
const MAX_RETRIES = 3;
const AXIOS_TIMEOUT = 120000;

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error('❌ SUPABASE_URL ou SUPABASE_KEY não configuradas');
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
  auth: { persistSession: false }
});

// ═══════════════════════════════════════════
// UTILITÁRIOS DE LEITURA
// ═══════════════════════════════════════════

function lerTexto(campo) {
  if (campo === undefined || campo === null) return '';
  if (typeof campo === 'object') {
    if (campo['#text'] !== undefined) return String(campo['#text']).trim();
    return '';
  }
  return String(campo).trim();
}

function lerNumero(campo) {
  if (campo === undefined || campo === null) return 0;
  if (typeof campo === 'object') {
    if (campo['#text'] !== undefined) {
      const val = parseFloat(campo['#text']);
      return isNaN(val) ? 0 : val;
    }
    return 0;
  }
  const val = parseFloat(campo);
  return isNaN(val) ? 0 : val;
}

function lerInteiro(campo) {
  return Math.floor(lerNumero(campo));
}

function extrairFotos(mediaNode) {
  if (!mediaNode || !mediaNode.Item) return [];
  const items = Array.isArray(mediaNode.Item) ? mediaNode.Item : [mediaNode.Item];
  const fotos = [];
  let capa = null;
  for (const m of items) {
    const url = lerTexto(m);
    if (!url || !url.startsWith('http')) continue;
    const isPrimary = m['@_primary'] === 'true' || m['@_primary'] === true;
    if (isPrimary && !capa) capa = url;
    else fotos.push(url);
  }
  if (capa) fotos.unshift(capa);
  return fotos;
}

function extrairDiferenciais(detailsNode) {
  if (!detailsNode || !detailsNode.Features || !detailsNode.Features.Feature) return [];
  const feat = detailsNode.Features.Feature;
  const lista = Array.isArray(feat) ? feat : [feat];
  return lista.map(f => lerTexto(f)).filter(f => f !== '').sort();
}

// ═══════════════════════════════════════════
// HASH DETERMINÍSTICO (VERIFICA ALTERAÇÕES)
// ═══════════════════════════════════════════

function gerarHash(d) {
  // Incluímos todos os campos que podem sofrer alteração
  const partes = [
    d.titulo, d.tipo, d.finalidade, d.cidade, d.bairro, d.cep, d.endereco, d.numero, d.uf,
    d.latitude, d.longitude, d.quartos, d.suites, d.banheiros, d.vagas_garagem,
    Number(d.area_total).toFixed(2), Number(d.area_util).toFixed(2),
    Number(d.valor_venda).toFixed(2), Number(d.valor_aluguel).toFixed(2),
    Number(d.valor_condominio).toFixed(2), Number(d.iptu).toFixed(2),
    d.descricao, // Descrição completa para garantir detecção de mudanças textuais
    d.angariador_nome, d.angariador_email, d.angariador_telefone,
    JSON.stringify(d.fotos_urls),
    JSON.stringify(d.diferenciais)
  ];
  return crypto.createHash('md5').update(partes.join('|')).digest('hex');
}

// ═══════════════════════════════════════════
// PARSE DO IMÓVEL
// ═══════════════════════════════════════════

function parsearImovel(item) {
  const listing_id = lerTexto(item.ListingID);
  if (!listing_id) return null;

  const details = item.Details || {};
  const location = item.Location || {};
  const contact = item.ContactInfo || item.Publisher || {}; 
  const transacao = lerTexto(item.TransactionType);

  let valor_venda = 0, valor_aluguel = 0;
  const pVenda = lerNumero(details.ListPrice);
  const pAluguel = lerNumero(details.RentalPrice);

  if (transacao === 'For Rent') valor_aluguel = pAluguel || pVenda;
  else if (transacao === 'For Sale') valor_venda = pVenda;
  else { valor_venda = pVenda; valor_aluguel = pAluguel; }

  const cidade = lerTexto(location.City);
  const lat = lerTexto(location.Latitude), lng = lerTexto(location.Longitude);

  const dados = {
    listing_id,
    titulo: lerTexto(item.Title),
    tipo: lerTexto(details.PropertyType) || null,
    finalidade: transacao || null,
    cidade: cidade ? cidade.toUpperCase() : null,
    bairro: lerTexto(location.Neighborhood) || null,
    uf: lerTexto(location.State) || 'PR',
    cep: lerTexto(location.PostalCode) || null,
    endereco: lerTexto(location.Address) || null,
    numero: lerInteiro(location.StreetNumber), 
    latitude: (lat && lat !== '0') ? lat : null,
    longitude: (lng && lng !== '0') ? lng : null,
    quartos: lerInteiro(details.Bedrooms),
    suites: lerInteiro(details.Suites),
    banheiros: lerInteiro(details.Bathrooms),
    vagas_garagem: lerInteiro(details.Garage),
    area_total: lerNumero(details.LotArea),
    area_util: lerNumero(details.LivingArea),
    valor_venda,
    valor_aluguel,
    valor_condominio: lerNumero(details.PropertyAdministrationFee),
    iptu: lerNumero(details.YearlyTax) || lerNumero(details.MonthlyTax),
    descricao: lerTexto(details.Description) || null,
    angariador_nome: lerTexto(contact.Name) || null,
    angariador_email: lerTexto(contact.Email) || null,
    angariador_telefone: lerTexto(contact.Telephone) || null,
    fotos_urls: extrairFotos(item.Media),
    diferenciais: extrairDiferenciais(details),
    xml_provider: PROVIDER_NAME
  };

  dados.data_hash = gerarHash(dados);
  return dados;
}

// ═══════════════════════════════════════════
// OPERAÇÕES DE BANCO
// ═══════════════════════════════════════════

async function downloadXML() {
  for (let t = 1; t <= MAX_RETRIES; t++) {
    try {
      console.log(`📥 Baixando XML (${t}/${MAX_RETRIES})...`);
      const res = await axios.get(XML_URL, { timeout: AXIOS_TIMEOUT });
      return res.data;
    } catch (err) {
      if (t === MAX_RETRIES) throw err;
      await new Promise(r => setTimeout(r, Math.pow(2, t) * 1000));
    }
  }
}

async function carregarBanco() {
  console.log('🔍 Lendo banco de dados...');
  const mapa = new Map();
  let offset = 0;
  while (true) {
    const { data, error } = await supabase
      .from(TABELA_CACHE)
      .select('listing_id, data_hash, status, xml_provider')
      .range(offset, offset + 999);
    if (error) throw error;
    if (!data || data.length === 0) break;
    data.forEach(row => mapa.set(String(row.listing_id).trim(), {
      hash: row.data_hash,
      status: row.status,
      provider: row.xml_provider
    }));
    if (data.length < 1000) break;
    offset += 1000;
  }
  console.log(`✅ ${mapa.size} registros encontrados.`);
  return mapa;
}

async function upsertBatch(registros) {
  if (registros.length === 0) return;
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const { error } = await supabase
      .from(TABELA_CACHE)
      .upsert(registros.slice(i, i + BATCH_SIZE), { onConflict: 'listing_id' });
    if (error) throw new Error(`Erro no upsert: ${error.message}`);
  }
}

async function atualizarSyncBatch(ids, timestamp) {
  if (ids.length === 0) return;
  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    await supabase.from(TABELA_CACHE).update({ last_sync: timestamp }).in('listing_id', ids.slice(i, i + BATCH_SIZE));
  }
}

async function inativarAusentes(ids, timestamp) {
  if (ids.length === 0) return;
  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    await supabase.from(TABELA_CACHE).update({ status: 'inativo', data_ultima_alteracao: timestamp }).in('listing_id', ids.slice(i, i + BATCH_SIZE));
  }
}

async function registrarLog(stats) {
  try {
    await supabase.from(TABELA_LOGS).insert({
      data_execucao: new Date().toISOString(),
      status: stats.erro ? 'erro' : 'sucesso',
      total_xml: stats.totalXml,
      novos: stats.novos,
      atualizados: stats.atualizados,
      reativados: stats.reativados,
      removidos: stats.inativados,
      sem_alteracao: stats.semAlteracao,
      mensagem_erro: stats.mensagemErro
    });
    console.log('📝 Log registrado.');
  } catch (e) { console.error('⚠️ Falha ao logar:', e.message); }
}

// ═══════════════════════════════════════════
// EXECUÇÃO
// ═══════════════════════════════════════════

async function runSync() {
  const agora = new Date().toISOString();
  const stats = { totalXml: 0, novos: 0, atualizados: 0, reativados: 0, inativados: 0, semAlteracao: 0, erro: false, mensagemErro: null };

  try {
    const banco = await carregarBanco();
    const xml = await downloadXML();
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', trimValues: true });
    const json = parser.parse(xml);
    const raw = json?.ListingDataFeed?.Listings?.Listing;
    const listings = Array.isArray(raw) ? raw : (raw ? [raw] : []);
    
    stats.totalXml = listings.length;
    const idsNoXml = new Set();
    const paraUpsert = [];
    const idsSemAlteracao = [];

    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel || idsNoXml.has(imovel.listing_id)) continue;
      idsNoXml.add(imovel.listing_id);

      const ex = banco.get(imovel.listing_id);

      // LÓGICA DE SUBSTITUIÇÃO:
      // Se não existe -> Novo
      // Se existe mas mudou preço, quartos, etc (hash diferente) -> Atualiza (Substitui)
      // Se estava inativo -> Reativa e Atualiza (Substitui)
      if (!ex) {
        stats.novos++;
        paraUpsert.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else if (ex.hash !== imovel.data_hash || ex.status !== 'ativo' || ex.provider !== PROVIDER_NAME) {
        if (ex.status !== 'ativo') stats.reativados++; else stats.atualizados++;
        paraUpsert.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else {
        stats.semAlteracao++;
        idsSemAlteracao.push(imovel.listing_id);
      }
    }

    const idsParaInativar = [];
    for (const [id, d] of banco.entries()) {
      if (d.provider === PROVIDER_NAME && d.status === 'ativo' && !idsNoXml.has(id)) idsParaInativar.push(id);
    }
    stats.inativados = idsParaInativar.length;

    console.log(`📊 Resumo: Novos/Atualizados: ${paraUpsert.length} | Inativar: ${stats.inativados} | Mantidos: ${stats.semAlteracao}`);

    if (paraUpsert.length > 0) await upsertBatch(paraUpsert);
    if (idsSemAlteracao.length > 0) await atualizarSyncBatch(idsSemAlteracao, agora);
    if (idsParaInativar.length > 0) await inativarAusentes(idsParaInativar, agora);

    console.log('✅ Sincronização concluída com sucesso!');
    await registrarLog(stats);
  } catch (e) {
    console.error('💥 ERRO:', e.message);
    stats.erro = true; stats.mensagemErro = e.message;
    await registrarLog(stats);
    process.exit(1);
  }
}

runSync();
