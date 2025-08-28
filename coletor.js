const fs = require("fs");
const axios = require("axios");
const winston = require("winston");
const pLimit = require("p-limit");
const { Client } = require("pg");

// Configuração do logger
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [new winston.transports.Console()],
});

// Limite de requisições simultâneas
const limit = pLimit(5);

// Produtos a serem buscados
const produtosParaBuscar = ["Arroz", "Feijão", "Açúcar", "Café", "Leite"];

// Supermercados (CNPJs) de exemplo
const supermercados = [
  { nome: "Mercado Popular", cnpj: "12345678000199" },
  { nome: "Super Econômico", cnpj: "98765432000188" },
];

// Função para normalizar nomes
function normalizarNome(nome) {
  return nome.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

// Função que busca dados de um produto em um mercado
async function buscarProduto(mercado, produto) {
  try {
    const response = await axios.get("https://api.economizaja.al.gov.br/produtos", {
      params: {
        cnpj: mercado.cnpj,
        nome: produto,
        limit: 5,
      },
      headers: {
        Authorization: `Bearer ${process.env.ECONOMIZA_ALAGOAS_TOKEN}`,
      },
    });

    return response.data.map((item) => {
      const dataColeta = new Date();
      return {
        id_registro: `${item.codigo_barras}_${dataColeta.getTime()}`,
        nome_supermercado: mercado.nome,
        cnpj_supermercado: mercado.cnpj,
        categoria_supermercado: item.categoria_supermercado || null,
        cidade_supermercado: item.cidade_supermercado || null,
        nome_produto: item.nome,
        nome_produto_normalizado: normalizarNome(item.nome),
        id_produto: item.id,
        categoria_produto: item.categoria || null,
        preco_produto: item.preco,
        unidade_medida: item.unidade || "un",
        data_ultima_venda: item.data_ultima_venda || null,
        data_coleta: dataColeta.toISOString(),
        codigo_barras: item.codigo_barras || null,
        origem: "API Economia AL",
        versao_script: "1.0.0",
      };
    });
  } catch (err) {
    logger.error(`Erro ao buscar ${produto} no ${mercado.nome}: ${err.message}`);
    return [];
  }
}

// Função para salvar no PostgreSQL
async function salvarNoBanco(registros) {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
  });

  await client.connect();

  for (const r of registros) {
    try {
      await client.query(
        `INSERT INTO registros_produtos (
          id_registro, nome_supermercado, cnpj_supermercado, categoria_supermercado, cidade_supermercado,
          nome_produto, nome_produto_normalizado, id_produto, categoria_produto, preco_produto,
          unidade_medida, data_ultima_venda, data_coleta, codigo_barras, origem, versao_script
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (id_registro) DO NOTHING`,
        [
          r.id_registro,
          r.nome_supermercado,
          r.cnpj_supermercado,
          r.categoria_supermercado,
          r.cidade_supermercado,
          r.nome_produto,
          r.nome_produto_normalizado,
          r.id_produto,
          r.categoria_produto,
          r.preco_produto,
          r.unidade_medida,
          r.data_ultima_venda ? new Date(r.data_ultima_venda) : null,
          r.data_coleta ? new Date(r.data_coleta) : null,
          r.codigo_barras,
          r.origem,
          r.versao_script,
        ]
      );
    } catch (err) {
      logger.error("Erro ao salvar no banco: " + err.message);
    }
  }

  await client.end();
}

// Função principal de coleta
async function coletarDadosMercado() {
  const historico = [];

  const tarefas = supermercados.flatMap((mercado) =>
    produtosParaBuscar.map((produto) =>
      limit(async () => {
        const dados = await buscarProduto(mercado, produto);
        historico.push(...dados);
      })
    )
  );

  await Promise.all(tarefas);

  // Salvar JSON (backup)
  const nomeArquivo = `coleta_${new Date().toISOString().slice(0, 10)}.json`;
  fs.writeFileSync(nomeArquivo, JSON.stringify(historico, null, 2));
  logger.info(`Arquivo salvo: ${nomeArquivo}`);

  // Salvar no banco
  await salvarNoBanco(historico);

  return historico;
}

// Exporta função main pro server.js
async function main() {
  await coletarDadosMercado();
}

module.exports = { main };
