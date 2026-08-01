/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 9.6 (Tratamento BigInt + Flexibilidade de XML)
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

// ═══════════════════════════════════════════
// VALIDAÇÃO DE AMBIENTE
// ═══════════════════════════════════════════
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error('❌ SUPABASE_URL ou SUPABASE_KEY não configuradas no .env');
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
  auth: { persistSession: false }
});

// ═══════════════════════════════════════════
// FUNÇÕES DE LEITURA DO XML
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
  const val = lerNumero(campo);
  return Math.floor(val);
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
    
    if (isPrimary && !capa) {
      capa = url;
    } else {
      fotos.push(url);
    }
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
// HASH DETERMINÍSTICO COM TRATAMENTO BIGINT
// ═══════════════════════════════════════════

function safeStringify(obj) {
  return JSON.stringify(obj, (key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  );
}

function gerarHash(d) {
  const partes = [
    String(d.titulo || ''),
    String(d.tipo || ''),
    String(d.finalidade || ''),
    String(d.cidade || '').toUpperCase(),
    String(d.bairro || ''),
    String(d.cep || ''),
    String(d.endereco || ''),
    String(d.numero || ''), 
    String(d.uf || ''),
    String(d.latitude || ''),
    String(d.longitude || ''),
    String(d.quartos || 0),
    String(d.suites || 0),
    String(d.banheiros || 0),
    String(d.vagas_garagem || 0),
    Number(d.area_total || 0).toFixed(2),
    Number(d.area_util || 0).toFixed(2),
    Number(d.valor_venda || 0).toFixed(2),
    Number(d.valor_aluguel || 0).toFixed(2),
    Number(d.valor_condominio || 0).toFixed(2),
    Number(d.iptu || 0).toFixed(2),
    String(d.descricao || ''),
    String(d.angariador_nome || ''),
    String(d.angariador_email || ''),
    String(d.angariador_telefone || ''),
    safeStringify(d.fotos_urls || []),
    safeStringify(d.diferenciais || [])
  ];

  const str = partes.join('|');
  return crypto.createHash('md5').update(str).digest('hex');
}

// ═══════════════════════════════════════════
// PARSEAR UM LISTING DO XML → OBJETO PADRONIZADO
// ═══════════════════════════════════════════

function parsearImovel(item) {
  const listing_id = lerTexto(item.ListingID);
  if (!listing_id) return null;

  const details = item.Details || {};
  const location = item.Location || {};
  const contact = item.ContactInfo || item.Publisher || {}; 

  const transacao = lerTexto(item.TransactionType);

  let valor_venda = 0;
  let valor_aluguel = 0;
  const pVenda = lerNumero(details.ListPrice);
  const pAluguel = lerNumero(details.RentalPrice);

  if (transacao === 'For Rent') {
    valor_aluguel = pAluguel || pVenda;
  } else if (transacao === 'For Sale') {
    valor_venda = pVenda;
  } else {
    valor_venda = pVenda;
    valor_aluguel = pAluguel;
  }

  const fotos = extrairFotos(item.Media);
  const diferenciais = extrairDiferenciais(details);

  const cidade = lerTexto(location.City);
  const lat = location.Latitude ? String(location.Latitude) : null;
  const lng = location.Longitude ? String(location.Longitude) : null;
  const latitude = (lat && lat !== '' && lat !== '0') ? lat : null;
  const longitude = (lng && lng !== '' && lng !== '0') ? lng : null;

  const numero = lerInteiro(location.StreetNumber);

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
    numero, 
    latitude,
    longitude,
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
    fotos_urls: fotos,
    diferenciais,
    xml_provider: PROVIDER_NAME
  };

  dados.data_hash = gerarHash(dados);
  return dados;
}

// ═══════════════════════════════════════════
// DOWNLOAD DO XML COM RETRY
// ═══════════════════════════════════════════

async function downloadXML() {
  for (let tentativa = 1; tentativa <= MAX_RETRIES; tentativa++) {
    try {
      console.log(`📥 Baixando XML (tentativa ${tentativa}/${MAX_RETRIES})...`);
      const response = await axios.get(XML_URL, {
        timeout: AXIOS_TIMEOUT,
        headers: { 'Accept-Encoding': 'gzip, deflate, br' }
      });
      if (!response.data) throw new Error('Resposta vazia');
      console.log('✅ XML baixado com sucesso.');
      return response.data;
    } catch (err) {
      console.error(`⚠️ Falha: ${err.message}`);
      if (tentativa === MAX_RETRIES) throw err;
      const wait = Math.pow(2, tentativa) * 1000;
      console.log(`⏳ Aguardando ${wait / 1000}s...`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// ═══════════════════════════════════════════
// OPERAÇÕES DE BANCO
// ═══════════════════════════════════════════

async function carregarBanco() {
  console.log('🔍 Carregando registros do banco...');
  const mapa = new Map();
  let offset = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from(TABELA_CACHE)
      .select('listing_id, data_hash, status, xml_provider')
      .range(offset, offset + pageSize - 1);

    if (error) throw new Error(`Erro ao ler banco: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const row of data) {
      const id = String(row.listing_id).trim();
      mapa.set(id, {
        hash: row.data_hash || '',
        status: row.status || 'ativo',
        provider: row.xml_provider
      });
    }

    if (data.length < pageSize) break;
    offset += pageSize;
  }

  console.log(`✅ ${mapa.size} registros carregados do banco.`);
  return mapa;
}

async function upsertBatch(registros, tipo = "Processando") {
  if (registros.length === 0) return;
  console.log(`🚀 ${tipo} ${registros.length} registros...`);
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from(TABELA_CACHE)
      .upsert(batch, { onConflict: 'listing_id' });
    if (error) throw error;
  }
  console.log(`   ✅ ${tipo} concluído.`);
}

async function atualizarSyncBatch(listingIds, timestamp) {
  if (listingIds.length === 0) return;
  for (let i = 0; i < listingIds.length; i += BATCH_SIZE) {
    await supabase.from(TABELA_CACHE).update({ last_sync: timestamp }).in('listing_id', listingIds.slice(i, i + BATCH_SIZE));
  }
}

async function inativarAusentes(listingIds, timestamp) {
  if (listingIds.length === 0) return;
  for (let i = 0; i < listingIds.length; i += BATCH_SIZE) {
    await supabase.from(TABELA_CACHE).update({ status: 'inativo', data_ultima_alteracao: timestamp }).in('listing_id', listingIds.slice(i, i + BATCH_SIZE));
  }
}

async function registrarLog(stats) {
  try {
    const payload = {
      data_execucao: new Date().toISOString(),
      status: stats.erro ? 'erro' : 'sucesso',
      total_xml: stats.totalXml,
      novos: stats.novos,
      atualizados: stats.atualizados,
      reativados: stats.reativados,
      removidos: stats.inativados,
      sem_alteracao: stats.semAlteracao,
      mensagem_erro: stats.mensagemErro || null
    };
    await supabase.from(TABELA_LOGS).insert(payload);
    console.log('📝 Log salvo no banco.');
  } catch (err) {
    console.error('⚠️ Erro ao salvar log:', err.message);
  }
}

// ═══════════════════════════════════════════
// PROCESSO PRINCIPAL
// ═══════════════════════════════════════════

async function runSync() {
  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🚀 SYNC XML v9.6 — TRATAMENTO BIGINT');
  console.log(`📅 ${new Date().toISOString()}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const agora = new Date().toISOString();
  const stats = { totalXml: 0, novos: 0, atualizados: 0, reativados: 0, inativados: 0, semAlteracao: 0, erro: false, mensagemErro: null };

  try {
    const banco = await carregarBanco();
    const xmlRaw = await downloadXML();
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', trimValues: true });
    const jsonData = parser.parse(xmlRaw);

    // FLEXIBILIDADE NA ESTRUTURA DO XML
    let listings = [];
    if (jsonData?.ListingDataFeed?.Listings?.Listing) {
      const raw = jsonData.ListingDataFeed.Listings.Listing;
      listings = Array.isArray(raw) ? raw : [raw];
    } else if (jsonData?.Listings?.Listing) {
      const raw = jsonData.Listings.Listing;
      listings = Array.isArray(raw) ? raw : [raw];
    } else {
      throw new Error('Estrutura do XML desconhecida (não encontrou Listings/Listing)');
    }

    stats.totalXml = listings.length;
    console.log(`📦 ${stats.totalXml} imóveis encontrados no XML.`);

    const idsNoXml = new Set();
    const paraUpsert = [];
    const idsSemAlteracao = [];

    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel || idsNoXml.has(imovel.listing_id)) continue;
      idsNoXml.add(imovel.listing_id);

      const ex = banco.get(imovel.listing_id);

      if (!ex) {
        stats.novos++;
        paraUpsert.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else if (ex.status !== 'ativo' || ex.hash !== imovel.data_hash || ex.provider !== PROVIDER_NAME) {
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

    console.log(`📊 Resumo: Novos: ${stats.novos} | Atualizados: ${stats.atualizados} | Reativados: ${stats.reativados} | Inativar: ${stats.inativados}`);

    if (paraUpsert.length > 0) await upsertBatch(paraUpsert, "Inserindo/Atualizando");
    if (idsSemAlteracao.length > 0) await atualizarSyncBatch(idsSemAlteracao, agora);
    if (idsParaInativar.length > 0) await inativarAusentes(idsParaInativar, agora);

    console.log('✅ Sincronização concluída com sucesso!');
    await registrarLog(stats);

  } catch (error) {
    console.error('💥 ERRO FATAL:', error.message);
    stats.erro = true; stats.mensagemErro = error.message.substring(0, 500);
    await registrarLog(stats);
    process.exit(1);
  }
}

runSync();
