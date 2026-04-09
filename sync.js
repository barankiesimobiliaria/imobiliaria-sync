/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 11.0 (Refatorada - Foco em Paginação e Inativação Segura)
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
const BATCH_SIZE = 50; // Mantido em 50 para evitar sobrecarga no Supabase
const TABELA_CACHE = 'cache_xml_externo';
const AXIOS_TIMEOUT = 120000;

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error('❌ SUPABASE_URL ou SUPABASE_KEY não configuradas no .env');
  process.exit(1);
}

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY, {
  auth: { persistSession: false }
});

// ═══════════════════════════════════════════
// FUNÇÕES AUXILIARES E PARSERS (Mantidas as originais)
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

function normalizarListingId(id) {
  // O trim() remove espaços invisíveis que quebram o comparativo
  return String(id || '').trim(); 
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
  return crypto.createHash('md5').update(partes.join('|')).digest('hex');
}

function parsearImovel(item) {
  const listing_id = normalizarListingId(lerTexto(item.ListingID));
  if (!listing_id) return null;

  const details = item.Details || {};
  const location = item.Location || {};
  const contact = item.ContactInfo || item.Publisher || {};
  const transacao = lerTexto(item.TransactionType);

  let valor_venda = 0;
  let valor_aluguel = 0;
  const pVenda = lerNumero(details.ListPrice);
  const pAluguel = lerNumero(details.RentalPrice);

  if (transacao === 'For Rent') valor_aluguel = pAluguel || pVenda;
  else if (transacao === 'For Sale') valor_venda = pVenda;
  else { valor_venda = pVenda; valor_aluguel = pAluguel; }

  const cidade = lerTexto(location.City);
  const lat = location.Latitude ? String(location.Latitude) : null;
  const lng = location.Longitude ? String(location.Longitude) : null;

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
// CORE: COMUNICAÇÃO ROBUSTA COM O BANCO
// ═══════════════════════════════════════════

async function carregarBancoSeguro() {
  console.log('🔍 Carregando imóveis do banco (paginação segura)...');
  const mapaBanco = new Map();
  let limit = 1000;
  let page = 0;
  let hasMore = true;

  while (hasMore) {
    const from = page * limit;
    const to = from + limit - 1;

    const { data, error } = await supabase
      .from(TABELA_CACHE)
      .select('listing_id, data_hash, status')
      .eq('xml_provider', PROVIDER_NAME)
      .range(from, to);

    if (error) {
      throw new Error(`Erro fatal ao ler banco na página ${page}: ${error.message}`);
    }

    if (!data || data.length === 0) {
      hasMore = false;
      break;
    }

    data.forEach(row => {
      mapaBanco.set(normalizarListingId(row.listing_id), {
        hash: row.data_hash || '',
        status: row.status || 'ativo'
      });
    });

    if (data.length < limit) hasMore = false;
    page++;
  }

  console.log(`✅ ${mapaBanco.size} registros carregados do banco com sucesso.`);
  return mapaBanco;
}

async function executarInativacoes(idsParaInativar, agora) {
  if (idsParaInativar.length === 0) return;
  
  console.log(`🗑️  Iniciando inativação de ${idsParaInativar.length} imóveis (sumiram do XML)...`);
  
  for (let i = 0; i < idsParaInativar.length; i += BATCH_SIZE) {
    const batch = idsParaInativar.slice(i, i + BATCH_SIZE);
    
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
      console.error(`⚠️ Erro ao inativar lote. Tentando inativar um a um... Detalhe: ${error.message}`);
      // Fallback: se o lote falhar, tenta um por um para não perder o processo
      for (const id of batch) {
        await supabase
          .from(TABELA_CACHE)
          .update({ status: 'inativo', data_ultima_alteracao: agora, last_sync: agora })
          .eq('listing_id', id)
          .eq('xml_provider', PROVIDER_NAME);
      }
    }
  }
  console.log(`✅ ${idsParaInativar.length} imóveis inativados com sucesso.`);
}

// ═══════════════════════════════════════════
// PROCESSO PRINCIPAL
// ═══════════════════════════════════════════

async function runSync() {
  console.log('\n🚀 INICIANDO SINCRONIZAÇÃO REFATORADA v11.0');
  const agora = new Date().toISOString();
  
  const stats = { totalXml: 0, novos: 0, atualizados: 0, reativados: 0, inativados: 0, erro: false };

  try {
    // 1. Carregar todo o histórico do banco de forma segura
    const mapaBanco = await carregarBancoSeguro();

    // 2. Baixar XML
    console.log('📥 Baixando XML...');
    const response = await axios.get(XML_URL, { timeout: AXIOS_TIMEOUT });
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', trimValues: true });
    const jsonData = parser.parse(response.data);
    
    const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
    const listings = Array.isArray(listingsRaw) ? listingsRaw : (listingsRaw ? [listingsRaw] : []);
    console.log(`📦 ${listings.length} imóveis lidos no XML.`);

    // Estruturas de controle
    const idsNoXml = new Set(); // Usando Set para busca ultra-rápida depois
    const paraInserir = [];
    const paraAtualizar = [];

    // 3. Processar Imóveis do XML (Inserts e Updates)
    for (const item of listings) {
      const imovel = parsearImovel(item);
      if (!imovel) continue;

      const id = imovel.listing_id;
      idsNoXml.add(id); // Guarda na memória que este ID VEIO no XML de hoje
      stats.totalXml++;

      const infoBanco = mapaBanco.get(id);

      if (!infoBanco) {
        // Não existe no banco -> NOVO
        stats.novos++;
        paraInserir.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else {
        // Existe no banco -> Verifica se precisa atualizar
        const mudou = infoBanco.hash !== imovel.data_hash || infoBanco.status !== 'ativo';
        
        if (mudou) {
          if (infoBanco.status !== 'ativo') stats.reativados++;
          else stats.atualizados++;
          
          paraAtualizar.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
        }
      }
    }

    // 4. Executar Inserts
    if (paraInserir.length > 0) {
      console.log(`🆕 Inserindo ${paraInserir.length} novos imóveis...`);
      for (let i = 0; i < paraInserir.length; i += BATCH_SIZE) {
        const batch = paraInserir.slice(i, i + BATCH_SIZE);
        const { error } = await supabase.from(TABELA_CACHE).insert(batch);
        if (error) console.error(`❌ Erro no insert: ${error.message}`);
      }
    }

    // 5. Executar Updates/Reativações
    if (paraAtualizar.length > 0) {
      console.log(`🔄 Atualizando ${paraAtualizar.length} imóveis...`);
      for (const imovel of paraAtualizar) {
        const { listing_id, ...dados } = imovel;
        const { error } = await supabase
          .from(TABELA_CACHE)
          .update(dados)
          .eq('listing_id', listing_id)
          .eq('xml_provider', PROVIDER_NAME);
        if (error) console.error(`❌ Erro no update do ID ${listing_id}: ${error.message}`);
      }
    }

    // 6. 🚨 LÓGICA 2 CORRIGIDA: Identificar e Inativar 🚨
    const idsParaInativar = [];
    
    // Varremos TUDO que estava no banco...
    for (const [idBanco, infoBanco] of mapaBanco.entries()) {
      // Se estava ATIVO no banco, MAS não veio no XML de hoje...
      if (infoBanco.status === 'ativo' && !idsNoXml.has(idBanco)) {
        idsParaInativar.push(idBanco);
      }
    }
    
    stats.inativados = idsParaInativar.length;
    await executarInativacoes(idsParaInativar, agora);

    console.log('\n✅ SINCRONIZAÇÃO CONCLUÍDA');
    console.log(JSON.stringify(stats, null, 2));

  } catch (error) {
    console.error('\n💥 ERRO FATAL:', error.message);
    process.exit(1);
  }
}

runSync();
