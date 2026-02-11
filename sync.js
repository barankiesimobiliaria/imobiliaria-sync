/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 9.0 (Reescrita completa)
 * 
 * REGRAS:
 *   1. Está no XML e NÃO está no banco        → INSERT (novo)
 *   2. Está no XML e está INATIVO no banco     → UPDATE reativar + atualizar dados
 *   3. NÃO está no XML e está ATIVO no banco   → UPDATE inativar
 *   4. Está no XML e está ATIVO no banco        → compara hash, se mudou → UPDATE dados
 * 
 * CORREÇÕES vs v8.1:
 *   - NUNCA faz upsert parcial (não destrói dados)
 *   - Hash determinístico e consistente
 *   - Separa INSERT de UPDATE (sem upsert misto)
 *   - Inativação por SET de IDs (não por timestamp)
 *   - Apenas atualiza last_sync nos sem alteração (via UPDATE, não upsert)
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
    // Lida com CDATA e atributos do fast-xml-parser
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
    
    // Verifica se é primary (pode vir como string ou boolean)
    const isPrimary = m['@_primary'] === 'true' || m['@_primary'] === true;
    
    if (isPrimary && !capa) {
      capa = url;
    } else {
      fotos.push(url);
    }
  }

  // Capa sempre na primeira posição
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
// HASH DETERMINÍSTICO
// Gera o mesmo hash se os dados forem iguais,
// independente de tipo (string vs number)
// ═══════════════════════════════════════════

function gerarHash(d) {
  const partes = [
    String(d.titulo || ''),
    String(d.tipo || ''),
    String(d.finalidade || ''),
    String(d.cidade || '').toUpperCase(),
    String(d.bairro || ''),
    String(d.endereco || ''),
    String(d.numero || ''), // Adicionado número aqui
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
    String(d.descricao || '').substring(0, 500),
    JSON.stringify(d.fotos_urls || []),
    JSON.stringify(d.diferenciais || [])
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
  const transacao = lerTexto(item.TransactionType);

  // Determinar valores de venda/aluguel
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
  // Limpa lat/lng vazias
  const latitude = (lat && lat !== '' && lat !== '0') ? lat : null;
  const longitude = (lng && lng !== '' && lng !== '0') ? lng : null;

  // Captura o StreetNumber (número da rua)
  const numero = lerInteiro(location.StreetNumber); // Novo campo

  const dados = {
    listing_id,
    titulo: lerTexto(item.Title),
    tipo: lerTexto(details.PropertyType) || null,
    finalidade: transacao || null,
    cidade: cidade ? cidade.toUpperCase() : null,
    bairro: lerTexto(location.Neighborhood) || null,
    uf: lerTexto(location.State) || 'PR',
    endereco: lerTexto(location.Address) || null,
    numero, // Adiciona o número aqui
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
// CARREGAR TODOS OS REGISTROS DO BANCO
// (listing_id, data_hash, status)
// ═══════════════════════════════════════════

async function carregarBanco() {
  console.log('🔍 Carregando registros do banco...');
  const mapa = new Map();
  let offset = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from(TABELA_CACHE)
      .select('listing_id, data_hash, status')
      .eq('xml_provider', PROVIDER_NAME)
      .range(offset, offset + pageSize - 1);

    if (error) throw new Error(`Erro ao ler banco: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const row of data) {
      const id = String(row.listing_id).trim();
      mapa.set(id, {
        hash: row.data_hash || '',
        status: row.status || 'ativo'
      });
    }

    if (data.length < pageSize) break;
    offset += pageSize;
  }

  console.log(`✅ ${mapa.size} registros carregados do banco.`);
  return mapa;
}

// ═══════════════════════════════════════════
// OPERAÇÕES EM BATCH NO SUPABASE
// ═══════════════════════════════════════════

async function inserirBatch(registros) {
  if (registros.length === 0) return;
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from(TABELA_CACHE)
      .insert(batch);
    if (error) {
      console.error(`❌ Erro no insert batch ${i}: ${error.message}`);
      throw error;
    }
  }
}

async function atualizarBatch(registros) {
  // Cada registro precisa ser atualizado individualmente por listing_id
  if (registros.length === 0) return;
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);
    const promises = batch.map(reg => {
      const { listing_id, ...dados } = reg;
      return supabase
        .from(TABELA_CACHE)
        .update(dados)
        .eq('listing_id', listing_id)
        .eq('xml_provider', PROVIDER_NAME);
    });
    const results = await Promise.all(promises);
    for (const { error } of results) {
      if (error) {
        console.error(`❌ Erro no update: ${error.message}`);
        throw error;
      }
    }
  }
}

async function atualizarSyncBatch(listingIds, timestamp) {
  // Apenas atualiza last_sync para quem não mudou (sem tocar nos dados!)
  if (listingIds.length === 0) return;
  for (let i = 0; i < listingIds.length; i += BATCH_SIZE) {
    const batch = listingIds.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from(TABELA_CACHE)
      .update({ last_sync: timestamp })
      .eq('xml_provider', PROVIDER_NAME)
      .in('listing_id', batch);
    if (error) {
      console.error(`❌ Erro ao atualizar last_sync: ${error.message}`);
      throw error;
    }
  }
}

async function inativarAusentes(listingIds, timestamp) {
  if (listingIds.length === 0) return;
  for (let i = 0; i < listingIds.length; i += BATCH_SIZE) {
    const batch = listingIds.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from(TABELA_CACHE)
      .update({
        status: 'inativo',
        data_ultima_alteracao: timestamp
      })
      .eq('xml_provider', PROVIDER_NAME)
      .in('listing_id', batch);
    if (error) {
      console.error(`❌ Erro ao inativar: ${error.message}`);
      throw error;
    }
  }
}

// ═══════════════════════════════════════════
// LOG DE EXECUÇÃO
// ═══════════════════════════════════════════

async function registrarLog(stats) {
  try {
    const { error } = await supabase.from(TABELA_LOGS).insert({
      data_execucao: new Date().toISOString(),
      status: stats.erro ? 'erro' : 'sucesso',
      total_xml: stats.totalXml,
      novos: stats.novos,
      atualizados: stats.atualizados,
      reativados: stats.reativados,
      removidos: stats.inativados,
      sem_alteracao: stats.semAlteracao,
      mensagem_erro: stats.mensagemErro || null
    });
    if (error) throw error;
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
  console.log('🚀 SYNC XML v9.0 — INÍCIO');
  console.log(`📅 ${new Date().toISOString()}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const agora = new Date().toISOString();

  const stats = {
    totalXml: 0,
    novos: 0,
    atualizados: 0,
    reativados: 0,
    inativados: 0,
    semAlteracao: 0,
    erro: false,
    mensagemErro: null
  };

  try {
    // ─── 1. Carregar banco atual ───
    const banco = await carregarBanco();

    // ─── 2. Baixar e parsear XML ───
    const xmlRaw = await downloadXML();
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '@_',
      trimValues: true
    });
    const jsonData = parser.parse(xmlRaw);

    const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
    if (!listingsRaw) throw new Error('Estrutura do XML inválida (sem Listings/Listing)');

    const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
    stats.totalXml = listings.length;
    console.log(`📦 ${stats.totalXml} imóveis encontrados no XML.`);

    // ─── 3. Processar cada imóvel do XML ───
    const idsNoXml = new Set();
    const paraInserir = [];       // Regra 1: novos
    const paraAtualizar = [];     // Regra 2 (reativar) e Regra 4 (dados mudaram)
    const idsSemAlteracao = [];   // Regra 4: sem mudança, só atualiza last_sync

    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel) continue;

      const { listing_id } = imovel;

      // Evitar duplicados dentro do próprio XML
      if (idsNoXml.has(listing_id)) continue;
      idsNoXml.add(listing_id);

      const existente = banco.get(listing_id);

      if (!existente) {
        // ═══ REGRA 1: Novo imóvel ═══
        stats.novos++;
        paraInserir.push({
          ...imovel,
          status: 'ativo',
          last_sync: agora,
          data_ultima_alteracao: agora
        });
      } else if (existente.status !== 'ativo') {
        // ═══ REGRA 2: Reativar ═══
        stats.reativados++;
        paraAtualizar.push({
          listing_id,
          ...imovel,
          status: 'ativo',
          last_sync: agora,
          data_ultima_alteracao: agora
        });
      } else if (existente.hash !== imovel.data_hash) {
        // ═══ REGRA 4a: Ativo mas dados mudaram ═══
        stats.atualizados++;
        paraAtualizar.push({
          listing_id,
          ...imovel,
          status: 'ativo',
          last_sync: agora,
          data_ultima_alteracao: agora
        });
      } else {
        // ═══ REGRA 4b: Ativo e sem alteração ═══
        stats.semAlteracao++;
        idsSemAlteracao.push(listing_id);
      }
    }

    // ─── REGRA 3: Inativar ausentes ───
    // Quem está ativo no banco mas NÃO apareceu no XML
    const idsParaInativar = [];
    for (const [listing_id, dados] of banco.entries()) {
      if (dados.status === 'ativo' && !idsNoXml.has(listing_id)) {
        idsParaInativar.push(listing_id);
      }
    }
    stats.inativados = idsParaInativar.length;

    // ─── 4. Executar operações no banco ───
    console.log('');
    console.log('📊 Resumo das operações:');
    console.log(`   🆕 Novos para inserir:    ${stats.novos}`);
    console.log(`   🔄 Para atualizar:        ${stats.atualizados}`);
    console.log(`   ♻️  Para reativar:         ${stats.reativados}`);
    console.log(`   🗑️  Para inativar:         ${stats.inativados}`);
    console.log(`   ✅ Sem alteração:          ${stats.semAlteracao}`);
    console.log('');

    // 4a. INSERIR novos
    if (paraInserir.length > 0) {
      console.log(`🆕 Inserindo ${paraInserir.length} novos...`);
      await inserirBatch(paraInserir);
      console.log('   ✅ Inserção concluída.');
    }

    // 4b. ATUALIZAR (mudanças + reativações)
    if (paraAtualizar.length > 0) {
      console.log(`🔄 Atualizando ${paraAtualizar.length} registros...`);
      await atualizarBatch(paraAtualizar);
      console.log('   ✅ Atualização concluída.');
    }

    // 4c. ATUALIZAR last_sync dos sem alteração
    if (idsSemAlteracao.length > 0) {
      console.log(`⏱️  Atualizando last_sync de ${idsSemAlteracao.length} sem alteração...`);
      await atualizarSyncBatch(idsSemAlteracao, agora);
      console.log('   ✅ last_sync atualizado.');
    }

    // 4d. INATIVAR ausentes
    if (idsParaInativar.length > 0) {
      console.log(`🗑️  Inativando ${idsParaInativar.length} ausentes...`);
      await inativarAusentes(idsParaInativar, agora);
      console.log('   ✅ Inativação concluída.');
    }

    // ─── 5. Resultado final ───
    console.log('');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('✅ SINCRONIZAÇÃO CONCLUÍDA COM SUCESSO!');
    console.log(`   📦 Total XML:       ${stats.totalXml}`);
    console.log(`   🆕 Novos:           ${stats.novos}`);
    console.log(`   🔄 Atualizados:     ${stats.atualizados}`);
    console.log(`   ♻️  Reativados:      ${stats.reativados}`);
    console.log(`   🗑️  Inativados:      ${stats.inativados}`);
    console.log(`   ✅ Sem alteração:    ${stats.semAlteracao}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    await registrarLog(stats);

  } catch (error) {
    console.error('');
    console.error('💥 ERRO FATAL:', error.message);
    console.error(error.stack);
    stats.erro = true;
    stats.mensagemErro = error.message.substring(0, 500);
    await registrarLog(stats);
    process.exit(1);
  }
}

runSync();
