/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 10.0 (SOLUÇÃO DEFINITIVA PARA INATIVAÇÃO)
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
  console.error('❌ SUPABASE_URL ou SUPABASE_KEY não configuradas no .env');
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
  auth: { persistSession: false }
});

// ═══════════════════════════════════════════
// FUNÇÕES AUXILIARES
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

/**
 * 🔧 NORMALIZAÇÃO CRÍTICA: 
 * Garante que o ID seja tratado da mesma forma em todas as etapas (XML e Banco).
 */
function normalizarListingId(id) {
  return String(id || '').trim(); // Removido .toLowerCase() para evitar conflitos se o banco for case-sensitive, mas mantido trim
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
// HASH DETERMINÍSTICO
// ═══════════════════════════════════════════

function gerarHash(d) {
  const partes = [
    String(d.titulo || '').trim(),
    String(d.tipo || '').trim(),
    String(d.finalidade || '').trim(),
    String(d.cidade || '').trim().toUpperCase(),
    String(d.bairro || '').trim(),
    String(d.cep || '').trim(),
    String(d.endereco || '').trim(),
    String(d.numero || 0),
    String(d.uf || 'PR').trim(),
    String(d.latitude || '').trim(),
    String(d.longitude || '').trim(),
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
    String(d.descricao || '').substring(0, 500).trim(),
    String(d.angariador_nome || '').trim(),
    String(d.angariador_email || '').trim(),
    String(d.angariador_telefone || '').trim(),
    JSON.stringify((d.fotos_urls || []).sort()),
    JSON.stringify((d.diferenciais || []).sort())
  ];

  const str = partes.join('|');
  return crypto.createHash('md5').update(str).digest('hex');
}

// ═══════════════════════════════════════════
// PARSEAR UM LISTING DO XML
// ═══════════════════════════════════════════

function parsearImovel(item) {
  const rawId = lerTexto(item.ListingID);
  const listing_id = normalizarListingId(rawId);
  
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
    titulo: lerTexto(item.Title) || 'Sem título',
    tipo: lerTexto(details.PropertyType) || null,
    finalidade: transacao || null,
    cidade: cidade ? cidade.toUpperCase() : 'DESCONHECIDA',
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
// OPERAÇÕES NO SUPABASE
// ═══════════════════════════════════════════

async function carregarBanco() {
  console.log('🔍 Carregando IDs ativos do banco...');
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
      // Importante: usar o listing_id original do banco para garantir o match no update posterior
      mapa.set(normalizarListingId(row.listing_id), {
        originalId: row.listing_id,
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

async function processarOperacoes(paraInserir, paraAtualizar, paraInativar, agora) {
  // 1. INSERIR NOVOS
  if (paraInserir.length > 0) {
    console.log(`🆕 Inserindo ${paraInserir.length} novos imóveis...`);
    for (let i = 0; i < paraInserir.length; i += BATCH_SIZE) {
      const batch = paraInserir.slice(i, i + BATCH_SIZE);
      const { error } = await supabase.from(TABELA_CACHE).insert(batch);
      if (error) console.error(`❌ Erro ao inserir batch: ${error.message}`);
    }
  }

  // 2. ATUALIZAR EXISTENTES (INCLUI REATIVADOS)
  if (paraAtualizar.length > 0) {
    console.log(`🔄 Atualizando/Reativando ${paraAtualizar.length} imóveis...`);
    // Updates individuais ou em batches pequenos para evitar timeout/erros de concorrência
    for (const imovel of paraAtualizar) {
      const { listing_id, ...dados } = imovel;
      const { error } = await supabase
        .from(TABELA_CACHE)
        .update(dados)
        .eq('listing_id', listing_id)
        .eq('xml_provider', PROVIDER_NAME);
      if (error) console.error(`❌ Erro ao atualizar ${listing_id}: ${error.message}`);
    }
  }

  // 3. INATIVAR AUSENTES (A CORREÇÃO)
  if (paraInativar.length > 0) {
    console.log(`🗑️  Inativando ${paraInativar.length} imóveis ausentes no XML...`);
    for (let i = 0; i < paraInativar.length; i += BATCH_SIZE) {
      const batch = paraInativar.slice(i, i + BATCH_SIZE);
      
      // TENTATIVA 1: Filtro .in()
      const { error } = await supabase
        .from(TABELA_CACHE)
        .update({ 
          status: 'inativo', 
          data_ultima_alteracao: agora,
          last_sync: agora 
        })
        .eq('xml_provider', PROVIDER_NAME)
        .in('listing_id', batch);

      if (error) {
        console.error(`⚠️ Erro no batch update de inativação: ${error.message}. Tentando individualmente...`);
        // Fallback: TENTATIVA 2: Individual
        for (const id of batch) {
          await supabase
            .from(TABELA_CACHE)
            .update({ status: 'inativo', data_ultima_alteracao: agora })
            .eq('listing_id', id)
            .eq('xml_provider', PROVIDER_NAME);
        }
      }
    }
    console.log('   ✅ Processo de inativação concluído.');
  }
}

// ═══════════════════════════════════════════
// PROCESSO PRINCIPAL
// ═══════════════════════════════════════════

async function runSync() {
  console.log('\n🚀 INICIANDO SINCRONIZAÇÃO OTIMIZADA v10.0');
  const agora = new Date().toISOString();
  
  const stats = { totalXml: 0, novos: 0, atualizados: 0, reativados: 0, inativados: 0, semAlteracao: 0, erro: false };

  try {
    // 1. Carregar Banco
    const banco = await carregarBanco();

    // 2. Baixar e Parsear XML
    console.log('📥 Baixando XML...');
    const response = await axios.get(XML_URL, { timeout: AXIOS_TIMEOUT });
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', trimValues: true });
    const jsonData = parser.parse(response.data);
    const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
    const listings = Array.isArray(listingsRaw) ? listingsRaw : (listingsRaw ? [listingsRaw] : []);
    
    console.log(`📦 ${listings.length} imóveis no XML.`);

    const idsNoXml = new Set();
    const paraInserir = [];
    const paraAtualizar = [];
    const idsSemAlteracao = [];

    // 3. Comparar XML -> Banco
    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel) continue;

      const id = imovel.listing_id;
      idsNoXml.add(id);
      stats.totalXml++;

      const existente = banco.get(id);

      if (!existente) {
        stats.novos++;
        paraInserir.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else {
        const mudou = existente.hash !== imovel.data_hash || existente.status !== 'ativo';
        
        if (mudou) {
          if (existente.status !== 'ativo') stats.reativados++;
          else stats.atualizados++;
          
          paraAtualizar.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
        } else {
          stats.semAlteracao++;
          idsSemAlteracao.push(existente.originalId);
        }
      }
    }

    // 4. Identificar o que INATIVAR (Banco -> XML)
    const idsParaInativar = [];
    for (const [id, dados] of banco.entries()) {
      if (dados.status === 'ativo' && !idsNoXml.has(id)) {
        idsParaInativar.push(dados.originalId);
      }
    }
    stats.inativados = idsParaInativar.length;

    // 5. Executar as operações
    await processarOperacoes(paraInserir, paraAtualizar, idsParaInativar, agora);

    // 6. Atualizar last_sync dos que não mudaram
    if (idsSemAlteracao.length > 0) {
      console.log(`⏱️  Sincronizando timestamp de ${idsSemAlteracao.length} imóveis...`);
      for (let i = 0; i < idsSemAlteracao.length; i += BATCH_SIZE) {
        await supabase.from(TABELA_CACHE).update({ last_sync: agora }).in('listing_id', idsSemAlteracao.slice(i, i + BATCH_SIZE)).eq('xml_provider', PROVIDER_NAME);
      }
    }

    console.log('\n✅ SINCRONIZAÇÃO CONCLUÍDA');
    console.log(JSON.stringify(stats, null, 2));

  } catch (error) {
    console.error('\n💥 ERRO FATAL:', error.message);
    process.exit(1);
  }
}

runSync();
