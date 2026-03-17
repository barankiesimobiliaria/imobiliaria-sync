/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 9.3 (Correções de consistência)
 *
 * Correções aplicadas:
 * 1. Hash estável: fotos e diferenciais são ordenados antes do stringify
 * 2. atualizarBatch: sequential por segurança, sem Promise.all sem controle
 * 3. Proteção contra XML vazio (0 listings não inativa tudo)
 * 4. Flag ALLOW_REACTIVATE para controle de reativação manual
 * 5. Logs mais detalhados para rastrear falhas
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');
const crypto = require('crypto');

const XML_URL = 'https://redeurbana.com.br/imoveis/rede/2e2b5834-643b-49c1-8289-005b800168e9';
const PROVIDER_NAME = 'RedeUrbana';
const BATCH_SIZE = 50;
const TABELA_CACHE = 'cache_xml_externo';
const TABELA_LOGS = 'import_logs';
const MAX_RETRIES = 3;
const AXIOS_TIMEOUT = 120000;

// ─── SEGURANÇA: só inativa se o XML trouxer pelo menos esse número de imóveis
// Evita inativar tudo se o XML vier vazio/corrompido
const MIN_LISTINGS_PARA_INATIVAR = 10;

// Se true, imóveis inativados manualmente voltam ao ativo quando reaparecem no XML
const ALLOW_REACTIVATE = true;

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
  if (!detailsNode?.Features?.Feature) return [];
  const feat = detailsNode.Features.Feature;
  const lista = Array.isArray(feat) ? feat : [feat];
  return lista.map(f => lerTexto(f)).filter(f => f !== '').sort();
}

// ═══════════════════════════════════════════
// HASH DETERMINÍSTICO
// FIX: fotos ordenadas antes do stringify para hash estável
// ═══════════════════════════════════════════

function gerarHash(d) {
  // Ordena fotos para garantir hash consistente independente da ordem retornada
  const fotosOrdenadas = [...(d.fotos_urls || [])].sort();
  const diferenciaisOrdenados = [...(d.diferenciais || [])].sort();

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
    String(d.descricao || '').substring(0, 500),
    String(d.angariador_nome || ''),
    String(d.angariador_email || ''),
    String(d.angariador_telefone || ''),
    JSON.stringify(fotosOrdenadas),
    JSON.stringify(diferenciaisOrdenados)
  ];

  return crypto.createHash('md5').update(partes.join('|')).digest('hex');
}

// ═══════════════════════════════════════════
// PARSEAR UM LISTING DO XML
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
      console.error(`⚠️ Falha tentativa ${tentativa}: ${err.message}`);
      if (tentativa === MAX_RETRIES) throw err;
      const wait = Math.pow(2, tentativa) * 1000;
      console.log(`⏳ Aguardando ${wait / 1000}s...`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// ═══════════════════════════════════════════
// CARREGAR BANCO
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
      mapa.set(String(row.listing_id).trim(), {
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
// OPERAÇÕES EM BATCH
// FIX: atualizarBatch sequencial por lotes, sem Promise.all descontrolado
// ═══════════════════════════════════════════

async function inserirBatch(registros) {
  if (registros.length === 0) return;
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);
    const { error } = await supabase.from(TABELA_CACHE).insert(batch);
    if (error) {
      console.error(`❌ Erro no insert batch [${i}..${i + batch.length - 1}]: ${error.message}`);
      throw error;
    }
    console.log(`   ✔ Inseridos ${Math.min(i + BATCH_SIZE, registros.length)}/${registros.length}`);
  }
}

async function atualizarBatch(registros) {
  if (registros.length === 0) return;
  let ok = 0;
  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);

    // Sequencial dentro do batch — evita race condition e facilita rastrear qual falhou
    for (const reg of batch) {
      const { listing_id, ...dados } = reg;
      const { error } = await supabase
        .from(TABELA_CACHE)
        .update(dados)
        .eq('listing_id', listing_id)
        .eq('xml_provider', PROVIDER_NAME);

      if (error) {
        console.error(`❌ Erro ao atualizar listing_id=${listing_id}: ${error.message}`);
        throw error;
      }
      ok++;
    }
    console.log(`   ✔ Atualizados ${ok}/${registros.length}`);
  }
}

async function atualizarSyncBatch(listingIds, timestamp) {
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
      .update({ status: 'inativo', data_ultima_alteracao: timestamp })
      .eq('xml_provider', PROVIDER_NAME)
      .in('listing_id', batch);
    if (error) {
      console.error(`❌ Erro ao inativar batch [${i}..${i + batch.length - 1}]: ${error.message}`);
      throw error;
    }
  }
}

// ═══════════════════════════════════════════
// LOG
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
  console.log('🚀 SYNC XML v9.3 — INÍCIO');
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
    const banco = await carregarBanco();

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

    // FIX: Proteção contra XML corrompido/vazio que inativaria tudo
    if (stats.totalXml < MIN_LISTINGS_PARA_INATIVAR) {
      throw new Error(
        `XML retornou apenas ${stats.totalXml} imóveis (mínimo esperado: ${MIN_LISTINGS_PARA_INATIVAR}). ` +
        `Abortando para evitar inativação em massa incorreta.`
      );
    }

    const idsNoXml = new Set();
    const paraInserir = [];
    const paraAtualizar = [];
    const idsSemAlteracao = [];

    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel) continue;

      const { listing_id } = imovel;

      // Ignora duplicatas dentro do mesmo XML
      if (idsNoXml.has(listing_id)) {
        console.warn(`⚠️ listing_id duplicado no XML ignorado: ${listing_id}`);
        continue;
      }
      idsNoXml.add(listing_id);

      const existente = banco.get(listing_id);

      if (!existente) {
        // Novo: não existe no banco
        stats.novos++;
        paraInserir.push({
          ...imovel,
          status: 'ativo',
          last_sync: agora,
          data_ultima_alteracao: agora
        });
      } else if (existente.status !== 'ativo') {
        // Estava inativo no banco mas voltou no XML
        if (ALLOW_REACTIVATE) {
          stats.reativados++;
          paraAtualizar.push({
            listing_id,
            ...imovel,
            status: 'ativo',
            last_sync: agora,
            data_ultima_alteracao: agora
          });
        } else {
          // Só atualiza last_sync, não muda status
          idsSemAlteracao.push(listing_id);
        }
      } else if (existente.hash !== imovel.data_hash) {
        // Dados mudaram
        stats.atualizados++;
        paraAtualizar.push({
          listing_id,
          ...imovel,
          status: 'ativo',
          last_sync: agora,
          data_ultima_alteracao: agora
        });
      } else {
        // Sem alteração
        stats.semAlteracao++;
        idsSemAlteracao.push(listing_id);
      }
    }

    // IDs no banco (ativos) que NÃO apareceram no XML → inativar
for (const [listing_id, dados] of banco.entries()) {
  if (dados.status === 'ativo' && !idsNoXml.has(listing_id)) {
    idsParaInativar.push(listing_id);
  }
}
    stats.inativados = idsParaInativar.length;

    console.log('');
    console.log('📊 Resumo das operações:');
    console.log(`   🆕 Novos para inserir:   ${stats.novos}`);
    console.log(`   🔄 Para atualizar:       ${stats.atualizados}`);
    console.log(`   ♻️  Para reativar:        ${stats.reativados}`);
    console.log(`   🗑️  Para inativar:        ${stats.inativados}`);
    console.log(`   ✅ Sem alteração:         ${stats.semAlteracao}`);
    console.log('');

    if (paraInserir.length > 0) {
      console.log(`🆕 Inserindo ${paraInserir.length} novos...`);
      await inserirBatch(paraInserir);
      console.log('   ✅ Inserção concluída.');
    }

    if (paraAtualizar.length > 0) {
      console.log(`🔄 Atualizando ${paraAtualizar.length} registros...`);
      await atualizarBatch(paraAtualizar);
      console.log('   ✅ Atualização concluída.');
    }

    if (idsSemAlteracao.length > 0) {
      console.log(`⏱️  Atualizando last_sync de ${idsSemAlteracao.length} sem alteração...`);
      await atualizarSyncBatch(idsSemAlteracao, agora);
      console.log('   ✅ last_sync atualizado.');
    }

    if (idsParaInativar.length > 0) {
      console.log(`🗑️  Inativando ${idsParaInativar.length} ausentes do XML...`);
      await inativarAusentes(idsParaInativar, agora);
      console.log('   ✅ Inativação concluída.');
    }

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
