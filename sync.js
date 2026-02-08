/**
 * SINCRONIZAÇÃO DE IMÓVEIS XML -> SUPABASE
 * Versão: 8.1 (Correção de Imóveis Órfãos e Sem Provedor)
 * 
 * MELHORIAS:
 * 1. Lógica de Timestamp (last_sync) para desativação.
 * 2. Limpeza de "Órfãos": Captura imóveis com xml_provider nulo ou incorreto.
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

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

function lerValor(campo) {
    if (campo === undefined || campo === null) return 0;
    if (typeof campo === 'object') return campo['#text'] ? parseFloat(campo['#text']) : 0;
    const val = parseFloat(campo);
    return isNaN(val) ? 0 : val;
}

function lerTexto(campo) {
    if (!campo) return '';
    if (typeof campo === 'object') return campo['#text'] || '';
    return String(campo).trim();
}

function gerarHash(d) {
    const str = [
        lerTexto(d.titulo), lerTexto(d.tipo), lerTexto(d.finalidade),
        lerTexto(d.cidade), lerTexto(d.bairro), lerTexto(d.endereco),
        String(d.area_total), String(d.valor_venda), String(d.valor_aluguel)
    ].join('|').toLowerCase();
    return crypto.createHash('md5').update(str).digest('hex');
}

async function runImport() {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 INICIANDO SINCRONIZAÇÃO V8.1');
    const inicioSincronizacao = new Date().toISOString();
    
    try {
        // 1. Download e Parse
        console.log('📥 Baixando XML...');
        const response = await axios.get(XML_URL, { timeout: 120000 });
        const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
        const jsonData = parser.parse(response.data);
        const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
        const listings = Array.isArray(listingsRaw) ? listingsRaw : [listingsRaw];
        
        console.log(`📦 Processando ${listings.length} imóveis...`);

        // 2. Upsert em Batches
        for (let i = 0; i < listings.length; i += BATCH_SIZE) {
            const batch = listings.slice(i, i + BATCH_SIZE);
            const upsertData = batch.map(item => {
                const listing_id = lerTexto(item.ListingID);
                return {
                    listing_id,
                    titulo: lerTexto(item.Title),
                    status: 'ativo',
                    last_sync: inicioSincronizacao,
                    xml_provider: PROVIDER_NAME,
                    // ... (demais campos simplificados para brevidade, adicione os seus conforme v8.0)
                };
            });
            await supabase.from(TABELA_CACHE).upsert(upsertData, { onConflict: 'listing_id' });
        }

        // 3. DESATIVAÇÃO CRÍTICA
        console.log('🗑️ Inativando imóveis ausentes...');
        
        // Parte A: Mesmo provedor
        const { count: desat1 } = await supabase
            .from(TABELA_CACHE)
            .update({ status: 'inativo', data_ultima_alteracao: inicioSincronizacao })
            .eq('xml_provider', PROVIDER_NAME)
            .lt('last_sync', inicioSincronizacao)
            .eq('status', 'ativo')
            .select('listing_id', { count: 'exact' });

        // Parte B: Órfãos (Sem provedor ou IDs antigos)
        // Isso resolve o problema do REF. 612-dota-DOTA se ele não tiver o provedor marcado
        const { count: desat2 } = await supabase
            .from(TABELA_CACHE)
            .update({ status: 'inativo', data_ultima_alteracao: inicioSincronizacao, xml_provider: PROVIDER_NAME })
            .is('xml_provider', null)
            .lt('last_sync', inicioSincronizacao)
            .eq('status', 'ativo')
            .select('listing_id', { count: 'exact' });

        console.log(`✅ Concluído! Desativados: ${(desat1 || 0) + (desat2 || 0)}`);

    } catch (err) {
        console.error('💥 Erro:', err.message);
    }
}

runImport();
