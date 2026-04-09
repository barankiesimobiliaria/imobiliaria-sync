// ... (parte anterior do código onde baixa o XML)
    const listingsRaw = jsonData?.ListingDataFeed?.Listings?.Listing;
    const listings = Array.isArray(listingsRaw) ? listingsRaw : (listingsRaw ? [listingsRaw] : []);
    
    console.log(`📦 TOTAL RAW NO XML: ${listings.length} imóveis.`); // <-- Aqui deve dar os 1357

    const idsNoXml = new Set();
    const paraInserir = [];
    const paraAtualizar = [];
    const idsSemAlteracao = []; 
    
    let descartadosSemId = 0;
    let duplicadosNoXml = 0;

    // 3. Processar Imóveis do XML (Inserts e Updates)
    for (const item of listings) {
      const imovel = parsearImovel(item);
      
      // Se a função parsearImovel retornar null, é porque falhou ao ler o ID
      if (!imovel) {
        descartadosSemId++;
        // Descomente a linha abaixo se quiser ver exatamente qual imóvel veio quebrado
        // console.log('⚠️ Imóvel ignorado (sem ID):', JSON.stringify(item).substring(0, 150));
        continue; 
      }

      const id = imovel.listing_id;
      
      // Verifica se o XML mandou o mesmo ID duas vezes
      if (idsNoXml.has(id)) {
        duplicadosNoXml++;
        console.log(`⚠️ Alerta: O XML mandou o ID ${id} mais de uma vez!`);
      }

      idsNoXml.add(id); 
      stats.totalXml++;

      const infoBanco = mapaBanco.get(id);

      if (!infoBanco) {
        stats.novos++;
        paraInserir.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
      } else {
        const mudou = infoBanco.hash !== imovel.data_hash || infoBanco.status !== 'ativo';
        
        if (mudou) {
          if (infoBanco.status !== 'ativo') stats.reativados++;
          else stats.atualizados++;
          
          paraAtualizar.push({ ...imovel, status: 'ativo', last_sync: agora, data_ultima_alteracao: agora });
        } else {
          stats.semAlteracao++;
          idsSemAlteracao.push(id);
        }
      }
    }

    console.log(`🔍 RELATÓRIO DO PARSER:`);
    console.log(`   - Descartados (Sem ID válido): ${descartadosSemId}`);
    console.log(`   - Duplicados (Mesmo ID no XML): ${duplicadosNoXml}`);
    // ... (segue para o passo 4)
