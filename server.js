const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');
const crypto = require('crypto');
const axios = require('axios');
const pLimit = require('p-limit').default;
const winston = require('winston');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Configuração do Supabase
const supabaseUrl = process.env.SUPABASE_URL || 'https://zhaetrzpkkgzfrwxfqdw.supabase.co';
const supabaseKey = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpoYWV0cnpwa2tnemZyd3hmcWR3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTc0MjM3MzksImV4cCI6MjA3Mjk5OTczOX0.UHoWWZahvp_lMDH8pK539YIAFTAUnQk9mBX5tdixwN0';
const supabase = createClient(supabaseUrl, supabaseKey);

// Configuração do logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(info => `[${info.timestamp}] ${info.level.toUpperCase()}: ${info.message}`)
  ),
  transports: [
    new winston.transports.File({ filename: path.join(__dirname, 'error.log'), level: 'error' }),
    new winston.transports.File({ filename: path.join(__dirname, 'combined.log') }),
    new winston.transports.Console({ format: winston.format.simple() }),
  ],
});

// Middlewares
app.use(cors({
  origin: ['https://seu-frontend.netlify.app', 'http://localhost:3000'],
  credentials: true
}));
app.use(express.json());

// Constantes da API Economiza Alagoas
const ECONOMIZA_ALAGOAS_TOKEN = process.env.ECONOMIZA_ALAGOAS_TOKEN || '0c80f47b7a0e3987fc8283c4a53e88c03191812a';
const ECONOMIZA_ALAGOAS_API_URL = 'http://api.sefaz.al.gov.br/sfz-economiza-alagoas-api/api/public/produto/pesquisa';

// Lista de mercados
const MERCADOS = [
  { nome: 'Popular Atacarejo', cnpj: '07771407000161', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Jomarte Atacarejo', cnpj: '13152804000158', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Azul Atacarejo', cnpj: '29457887000204', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Bella Compora Rua São João', cnpj: '07671615000431', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'Bella Compora Rua Do Sol', cnpj: '07671615000350', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'ATACADAO S.A', cnpj: '75315333014835', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'FELIX SUPERMERCADO', cnpj: '60590998000153', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADO MASTER', cnpj: '01635096000127', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADOS SÃO LUIZ Baixão', cnpj: '15353706000104', categoria: 'Atacarejo', cidade: 'Arapiraca' },
  { nome: 'SUPERMERCADOS SAO LUIZ Ceci Cunha', cnpj: '15353706000619', categoria: 'Atacarejo', cidade: 'Arapiraca' }
];

// Lista de produtos
const NOMES_PRODUTOS = [
  // Alimentos básicos
  'arroz', 'feijão', 'açúcar', 'óleo', 'café', 'macarrão', 'leite', 'pão', 'farinha', 'sal',
  'arroz integral', 'arroz parboilizado', 'feijão preto', 'feijão carioca', 'lentilha', 'ervilha', 'milho verde',
  'batata', 'batata doce', 'mandioca', 'aipim', 'abóbora', 'cenoura', 'beterraba', 'cebola', 'alho', 'tomate',
  'pepino', 'pimentão', 'abobrinha', 'alface', 'rúcula', 'espinafre',
  // Frutas
  'banana', 'maçã', 'laranja', 'limão', 'uva', 'mamão', 'abacaxi', 'melancia', 'melão', 'pera', 'manga',
  'goiaba', 'kiwi', 'caqui', 'ameixa', 'coco',
  // Carnes bovinas
  'carne', 'carne bovina', 'alcatra', 'contrafilé', 'coxão mole', 'coxão duro', 'patinho', 'maminha', 'fraldinha',
  'filé mignon', 'picanha', 'costela', 'músculo', 'lagarto', 'acém', 'paleta', 'cupim', 'aba de filé', 'bisteca bovina',
  'miolo da paleta', 'ossobuco', 'rabo bovino', 'fígado bovino', 'moela bovina', 'linguiça de boi', 'bife', 'carne moída',
  'rabada', 'vazio', 'matambre', 'peito bovino', 'capa de filé',
  // Carnes suínas
  'carne suína', 'lombo suíno', 'costelinha suína', 'pernil', 'paleta suína', 'bisteca suína', 'linguiça de porco',
  'panceta', 'torresmo', 'joelho de porco', 'pé de porco', 'orelha de porco', 'filé mignon suíno', 'copa lombo', 'costela suína',
  'linguiça calabresa', 'linguiça toscana',
  // Carnes de frango e aves
  'frango', 'peito de frango', 'coxa de frango', 'sobrecoxa', 'asa de frango', 'frango inteiro', 'filezinho sassami',
  'moela de frango', 'fígado de frango', 'coração de frango', 'pescoço de frango', 'pé de frango', 'galinha caipira',
  'peru', 'chester', 'pato', 'codorna', 'frango defumado',
  // Carnes de cordeiro/caprino
  'carne de cordeiro', 'costela de cordeiro', 'paleta de cordeiro', 'pernil de cordeiro', 'carneiro',
  'cabrito', 'paleta de cabrito', 'pernil de cabrito', 'costela de carneiro',
  // Peixes e frutos do mar
  'peixe', 'filé de peixe', 'tilápia', 'salmão', 'bacalhau', 'atum', 'sardinha', 'merluza', 'corvina', 'pescada', 'truta',
  'pirarucu', 'pintado', 'cação', 'anchova', 'dourado', 'tambaqui', 'camarão', 'lula', 'polvo', 'ostra', 'marisco',
  'mexilhão', 'caranguejo', 'lagosta', 'sirigado', 'arraia', 'surubim', 'bacalhau dessalgado', 'bacalhau salgado',
  // Embutidos e defumados
  'presunto', 'mortadela', 'salame', 'linguiça', 'salsicha', 'paio', 'blanquet', 'peito de peru', 'salaminho', 'copa',
  'presunto parma', 'presunto cru', 'pastrami', 'apresuntado', 'fiambre', 'defumado', 'pancetta',
  // Ovos e derivados
  'ovos', 'clara de ovo', 'gema de ovo',
  // Laticínios
  'manteiga', 'margarina', 'iogurte', 'creme de leite', 'leite condensado', 'requeijão', 'queijo minas', 'queijo prato',
  'queijo mussarela',
  // Padaria e confeitaria
  'bolo', 'massa para bolo', 'fermento', 'gelatina', 'sucrilhos', 'aveia', 'granola', 'biscoito', 'bolacha',
  'biscoito recheado', 'biscoito cream cracker', 'biscoito água e sal', 'biscoito de polvilho', 'pão de forma', 'torrada',
  // Doces, chocolates e snacks
  'chocolate', 'doce', 'balas', 'pirulito', 'pipoca', 'bala', 'barrinha de cereal', 'paçoca', 'amendoim', 'castanha',
  'nozes', 'uva passa',
  // Bebidas
  'refrigerante', 'suco', 'água mineral', 'cerveja', 'vinho', 'vodka', 'whisky', 'cachaça', 'energético', 'chá', 'mate',
  'isotônico',
  // Conservas e molhos
  'extrato de tomate', 'molho de tomate', 'maionese', 'ketchup', 'mostarda', 'azeite', 'vinagre', 'azeitona',
  'ervilha em lata', 'milho em lata', 'sardinha em lata', 'atum em lata',
  // Produtos de higiene e limpeza
  'sabão em pó', 'sabão em barra', 'detergente', 'amaciante', 'desinfetante', 'água sanitária', 'multiuso', 'esponja',
  'papel higiênico', 'guardanapo', 'toalha de papel', 'shampoo', 'condicionador', 'sabonete', 'creme dental',
  'escova de dente', 'desodorante', 'absorvente', 'fralda', 'papel toalha', 'alvejante', 'limpa vidro', 'lustra móveis',
  // Animais de estimação
  'ração', 'areia para gato', 'biscoito para cachorro',
  // Outros ingredientes
  'farinha de mandioca', 'farinha de trigo', 'fubá', 'polvilho', 'massa para pastel'
];

// Constantes de configuração
const DIAS_PESQUISA = 5;
const REGISTROS_POR_PAGINA = 50;
const INTERVALO_EM_HORAS = 24;
const LIMITE_DIAS_HISTORICO = 35;
const VERSAO_SCRIPT = "2.2.0-pastas-individuais";
const CONCORRENCIA_PRODUTOS = 10;
const CONCORRENCIA_MERCADOS = 5;
const RETRY_MAX = 3;
const RETRY_BASE_MS = 2000;
const BLACKLIST_RETRIES = 2;
const BLACKLIST_TEMPO_MS = 1000 * 60 * 30;

// Níveis de permissão
const PERMISSION_LEVELS = {
  ADMIN: 3,
  MODERATOR: 2,
  VISITOR: 1
};

// Middleware de autenticação JWT
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  
  if (!token) return res.status(401).json({ message: 'Token de autenticação não fornecido' });
  
  jwt.verify(token, process.env.JWT_SECRET || 'default_secret', (err, user) => {
    if (err) return res.status(403).json({ message: 'Token inválido ou expirado' });
    req.user = user;
    next();
  });
}

// Middleware de autorização por nível
function authorizeLevel(level) {
  return (req, res, next) => {
    if (req.user.permission_level < level) {
      return res.status(403).json({ message: 'Você não tem permissão para acessar este recurso' });
    }
    next();
  };
}

// Funções utilitárias
function normalizarTexto(txt) {
  if (!txt) return "";
  return txt
    .toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9 ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function gerarIdProduto(produto, unidade, codBarras) {
  if (codBarras && codBarras.length >= 8) return codBarras;
  return normalizarTexto(produto + "_" + (unidade || "")).replace(/\s/g, "_");
}

function gerarIdRegistro(obj) {
  const hash = crypto.createHash('sha1');
  hash.update([
    obj.cnpj_supermercado,
    obj.id_produto,
    obj.preco_produto,
    obj.data_ultima_venda,
    obj.data_coleta
  ].join('|'));
  return hash.digest('hex').substring(0, 16);
}

function isRegistroIgual(a, b) {
  return a.id_registro === b.id_registro;
}

function filtrarRegistrosRecentes(historico) {
  const agora = new Date();
  return historico.filter(registro => {
    if (!registro.data_coleta) return false;
    const dataColeta = new Date(registro.data_coleta);
    const diffDias = (agora - dataColeta) / (1000 * 60 * 60 * 24);
    return diffDias <= LIMITE_DIAS_HISTORICO;
  });
}

// Endpoint de teste
app.get('/api/test', async (req, res) => {
  try {
    const { data, error } = await supabase.from('users').select('count').single();
    
    if (error) {
      return res.status(500).json({ message: 'Erro ao conectar com o Supabase', error: error.message });
    }
    
    res.json({ message: 'Conexão com o Supabase bem-sucedida', data });
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

// Rotas de autenticação
app.post('/api/register', authenticateToken, authorizeLevel(PERMISSION_LEVELS.ADMIN), async (req, res) => {
  try {
    const { username, password, email, permission_level } = req.body;
    
    // Verificar se o usuário já existe
    const { data: existingUser, error: fetchError } = await supabase
      .from('users')
      .select('*')
      .eq('username', username)
      .single();
    
    if (existingUser) {
      return res.status(400).json({ message: 'Nome de usuário já em uso' });
    }
    
    // Hash da senha
    const salt = bcrypt.genSaltSync(10);
    const hashedPassword = bcrypt.hashSync(password, salt);
    
    // Inserir novo usuário
    const { data, error } = await supabase
      .from('users')
      .insert([
        { 
          username, 
          password: hashedPassword, 
          email,
          permission_level: permission_level || PERMISSION_LEVELS.VISITOR
        }
      ])
      .single();
    
    if (error) {
      return res.status(500).json({ message: 'Erro ao criar usuário', error: error.message });
    }
    
    // Registrar log
    await supabase
      .from('logs')
      .insert([
        {
          action: 'CREATE_USER',
          user_id: req.user.id,
          target_user_id: data.id,
          details: `Criou usuário ${username} com nível de permissão ${permission_level || PERMISSION_LEVELS.VISITOR}`
        }
      ]);
    
    res.status(201).json({
      message: 'Usuário criado com sucesso',
      user: {
        id: data.id,
        username: data.username,
        email: data.email,
        permission_level: data.permission_level
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    
    // Buscar usuário
    const { data: user, error } = await supabase
      .from('users')
      .select('*')
      .eq('username', username)
      .single();
    
    if (error || !user) {
      return res.status(401).json({ message: 'Credenciais inválidas' });
    }
    
    // Verificar senha
    const isPasswordValid = bcrypt.compareSync(password, user.password);
    if (!isPasswordValid) {
      return res.status(401).json({ message: 'Credenciais inválidas' });
    }
    
    // Gerar token JWT
    const token = jwt.sign({ 
      id: user.id, 
      username: user.username,
      permission_level: user.permission_level
    }, process.env.JWT_SECRET || 'default_secret', { expiresIn: '24h' });
    
    // Registrar log
    await supabase
      .from('logs')
      .insert([
        {
          action: 'LOGIN',
          user_id: user.id,
          details: `Usuário ${username} fez login`
        }
      ]);
    
    res.json({
      message: 'Login realizado com sucesso',
      token,
      user: {
        id: user.id,
        username: user.username,
        email: user.email,
        permission_level: user.permission_level
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

// Rotas de usuários
app.get('/api/users', authenticateToken, authorizeLevel(PERMISSION_LEVELS.ADMIN), async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('id, username, email, permission_level, created_at');
    
    if (error) {
      return res.status(500).json({ message: 'Erro ao buscar usuários', error: error.message });
    }
    
    res.json(data);
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

app.put('/api/users/:id', authenticateToken, authorizeLevel(PERMISSION_LEVELS.ADMIN), async (req, res) => {
  try {
    const { id } = req.params;
    const { permission_level } = req.body;
    
    const { data, error } = await supabase
      .from('users')
      .update({ permission_level })
      .eq('id', id)
      .select('id, username, email, permission_level')
      .single();
    
    if (error) {
      return res.status(500).json({ message: 'Erro ao atualizar usuário', error: error.message });
    }
    
    // Registrar log
    await supabase
      .from('logs')
      .insert([
        {
          action: 'UPDATE_USER',
          user_id: req.user.id,
          target_user_id: id,
          details: `Atualizou permissão do usuário ${data.username} para nível ${permission_level}`
        }
      ]);
    
    res.json({
      message: 'Usuário atualizado com sucesso',
      user: data
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

// Rotas de logs
app.get('/api/logs', authenticateToken, authorizeLevel(PERMISSION_LEVELS.ADMIN), async (req, res) => {
  try {
    const { page = 1, limit = 20 } = req.query;
    const offset = (page - 1) * limit;
    
    const { data, error, count } = await supabase
      .from('logs')
      .select(`
        *,
        user:users(username),
        target_user:target_user_id(username)
      `, { count: 'exact' })
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1);
    
    if (error) {
      return res.status(500).json({ message: 'Erro ao buscar logs', error: error.message });
    }
    
    res.json({
      data,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: count
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

// Rotas da API de produtos
app.get('/api/mercados', authenticateToken, async (req, res) => {
  try {
    res.json(MERCADOS);
  } catch (error) {
    res.status(500).json({ message: 'Erro ao buscar mercados', error: error.message });
  }
});

app.get('/api/produtos', authenticateToken, async (req, res) => {
  try {
    res.json(NOMES_PRODUTOS);
  } catch (error) {
    res.status(500).json({ message: 'Erro ao buscar produtos', error: error.message });
  }
});

app.get('/api/dados', authenticateToken, async (req, res) => {
  try {
    const { 
      cnpj, 
      produto, 
      codigoBarras,
      dataInicio, 
      dataFim,
      source = 'database' // 'database' ou 'api'
    } = req.query;
    
    let query;
    
    if (source === 'api') {
      // Buscar diretamente da API Economiza Alagoas
      const resultados = [];
      
      // Se um CNPJ foi especificado, buscar apenas nesse mercado
      const mercadosParaBuscar = cnpj 
        ? MERCADOS.filter(m => m.cnpj === cnpj)
        : MERCADOS;
      
      for (const mercado of mercadosParaBuscar) {
        const requestBody = {
          produto: { descricao: produto ? produto.toUpperCase() : '' },
          estabelecimento: { individual: { cnpj: mercado.cnpj } },
          dias: DIAS_PESQUISA,
          pagina: 1,
          registrosPorPagina: REGISTROS_POR_PAGINA
        };
        
        if (codigoBarras) {
          requestBody.produto.gtin = codigoBarras;
        }
        
        try {
          const response = await axios.post(ECONOMIZA_ALAGOAS_API_URL, requestBody, {
            headers: {
              'AppToken': ECONOMIZA_ALAGOAS_TOKEN,
              'Content-Type': 'application/json'
            },
            timeout: 50000
          });
          
          const conteudo = Array.isArray(response.data?.conteudo) ? response.data.conteudo : [];
          
          for (const item of conteudo) {
            const produtoInfo = item.produto || {};
            const venda = produtoInfo.venda || {};
            const nomeNormalizado = normalizarTexto(produtoInfo.descricao);
            const unidade = produtoInfo.unidadeMedida || '';
            const codBarras = produtoInfo.gtin || '';
            const id_produto = gerarIdProduto(produtoInfo.descricao, unidade, codBarras);
            
            const registro = {
              nome_supermercado: mercado.nome,
              cnpj_supermercado: mercado.cnpj,
              categoria_supermercado: mercado.categoria || "",
              cidade_supermercado: mercado.cidade || "",
              nome_produto: produtoInfo.descricao || '',
              nome_produto_normalizado: nomeNormalizado,
              id_produto,
              categoria_produto: produtoInfo.categoria || "",
              preco_produto: venda.valorVenda || '',
              unidade_medida: unidade,
              data_ultima_venda: venda.dataVenda || '',
              data_coleta: new Date().toISOString(),
              codigo_barras: codBarras,
              origem: "Economiza Alagoas API",
              versao_script: VERSAO_SCRIPT
            };
            
            registro.id_registro = gerarIdRegistro({
              cnpj_supermercado: mercado.cnpj,
              id_produto,
              preco_produto: venda.valorVenda || '',
              data_ultima_venda: venda.dataVenda || '',
              data_coleta: registro.data_coleta
            });
            
            resultados.push(registro);
          }
        } catch (error) {
          logger.error(`Erro ao consultar API para ${mercado.nome}: ${error.message}`);
        }
      }
      
      res.json(resultados);
    } else {
      // Buscar do banco de dados (Supabase)
      query = supabase
        .from('produtos_supermercados')
        .select('*');
      
      if (cnpj) {
        query = query.eq('cnpj_supermercado', cnpj);
      }
      
      if (produto) {
        query = query.ilike('nome_produto_normalizado', `%${normalizarTexto(produto)}%`);
      }
      
      if (codigoBarras) {
        query = query.eq('codigo_barras', codigoBarras);
      }
      
      if (dataInicio) {
        query = query.gte('data_coleta', dataInicio);
      }
      
      if (dataFim) {
        query = query.lte('data_coleta', dataFim);
      }
      
      const { data, error } = await query;
      
      if (error) {
        return res.status(500).json({ message: 'Erro ao buscar dados', error: error.message });
      }
      
      res.json(data);
    }
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

app.get('/api/produto/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    const { data, error } = await supabase
      .from('produtos_supermercados')
      .select('*')
      .eq('id', id)
      .single();
    
    if (error) {
      return res.status(404).json({ message: 'Produto não encontrado', error: error.message });
    }
    
    res.json(data);
  } catch (error) {
    res.status(500).json({ message: 'Erro no servidor', error: error.message });
  }
});

app.post('/api/coletar-dados', authenticateToken, authorizeLevel(PERMISSION_LEVELS.MODERATOR), async (req, res) => {
  try {
    // Iniciar coleta de dados
    res.json({ message: 'Coleta de dados iniciada em segundo plano' });
    
    // Registrar log
    await supabase
      .from('logs')
      .insert([
        {
          action: 'COLLECT_DATA',
          user_id: req.user.id,
          details: `Iniciou coleta de dados`
        }
      ]);
    
    // Executar coleta em segundo plano
    coletarDados().catch(error => {
      logger.error('Erro na coleta de dados: ' + error.message);
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro ao iniciar coleta de dados', error: error.message });
  }
});

app.get('/api/comparar', authenticateToken, async (req, res) => {
  try {
    const { 
      produtos, // Array de nomes de produtos
      mercados, // Array de CNPJs de mercados
      periodo = 7, // Período em dias
      source = 'database' // 'database' ou 'api'
    } = req.query;
    
    if (!produtos || !Array.isArray(produtos) || produtos.length === 0) {
      return res.status(400).json({ message: 'Nenhum produto especificado' });
    }
    
    if (produtos.length > 5) {
      return res.status(400).json({ message: 'É possível comparar no máximo 5 produtos' });
    }
    
    // Calcular período
    const dataFim = new Date();
    const dataInicio = new Date();
    dataInicio.setDate(dataFim.getDate() - parseInt(periodo));
    
    // Formatar datas para ISO
    const dataInicioISO = dataInicio.toISOString();
    const dataFimISO = dataFim.toISOString();
    
    const resultado = {};
    
    // Para cada produto, buscar dados
    for (const produto of produtos) {
      resultado[produto] = {};
      
      // Buscar dados do produto
      let dadosProduto;
      
      if (source === 'api') {
        // Buscar da API
        const response = await axios.get(`${API_URL}/dados`, {
          params: {
            produto,
            dataInicio: dataInicioISO,
            dataFim: dataFimISO,
            source: 'api'
          },
          headers: {
            'Authorization': `Bearer ${req.headers.authorization.split(' ')[1]}`
          }
        });
        
        dadosProduto = response.data;
      } else {
        // Buscar do banco de dados
        const { data, error } = await supabase
          .from('produtos_supermercados')
          .select('*')
          .ilike('nome_produto_normalizado', `%${normalizarTexto(produto)}%`)
          .gte('data_coleta', dataInicioISO)
          .lte('data_coleta', dataFimISO);
        
        if (error) {
          return res.status(500).json({ message: 'Erro ao buscar dados para comparação', error: error.message });
        }
        
        dadosProduto = data;
      }
      
      // Filtrar por mercados, se especificado
      if (mercados && Array.isArray(mercados) && mercados.length > 0) {
        dadosProduto = dadosProduto.filter(item => mercados.includes(item.cnpj_supermercado));
      }
      
      // Agrupar por supermercado
      const supermercados = {};
      
      dadosProduto.forEach(registro => {
        const cnpj = registro.cnpj_supermercado;
        if (!supermercados[cnpj]) {
          const mercado = MERCADOS.find(m => m.cnpj === cnpj);
          supermercados[cnpj] = {
            nome_supermercado: mercado ? mercado.nome : 'Desconhecido',
            categoria: mercado ? mercado.categoria : '',
            cidade: mercado ? mercado.cidade : '',
            registros: []
          };
        }
        supermercados[cnpj].registros.push(registro);
      });
      
      // Para cada supermercado, calcular estatísticas
      for (const cnpj in supermercados) {
        const supermercado = supermercados[cnpj];
        const registros = supermercado.registros;
        
        if (registros.length === 0) continue;
        
        // Ordenar por preço
        registros.sort((a, b) => parseFloat(a.preco_produto) - parseFloat(b.preco_produto));
        
        // Calcular estatísticas
        const precos = registros.map(r => parseFloat(r.preco_produto));
        const menorPreco = Math.min(...precos);
        const maiorPreco = Math.max(...precos);
        const precoMedio = precos.reduce((sum, p) => sum + p, 0) / precos.length;
        
        resultado[produto][cnpj] = {
          nome_supermercado: supermercado.nome_supermercado,
          categoria: supermercado.categoria,
          cidade: supermercado.cidade,
          menor_preco: menorPreco.toFixed(2),
          maior_preco: maiorPreco.toFixed(2),
          preco_medio: precoMedio.toFixed(2),
          quantidade_registros: registros.length,
          registros: registros
        };
      }
    }
    
    res.json(resultado);
  } catch (error) {
    res.status(500).json({ message: 'Erro ao comparar produtos', error: error.message });
  }
});

app.get('/api/estatisticas', authenticateToken, async (req, res) => {
  try {
    const { 
      cnpj, 
      periodo = 30, 
      source = 'database' 
    } = req.query;
    
    // Calcular período
    const dataFim = new Date();
    const dataInicio = new Date();
    dataInicio.setDate(dataFim.getDate() - parseInt(periodo));
    
    // Formatar datas para ISO
    const dataInicioISO = dataInicio.toISOString();
    const dataFimISO = dataFim.toISOString();
    
    // Buscar dados
    let dados;
    
    if (source === 'api') {
      const response = await axios.get(`${API_URL}/dados`, {
        params: {
          dataInicio: dataInicioISO,
          dataFim: dataFimISO,
          source: 'api'
        },
        headers: {
          'Authorization': `Bearer ${req.headers.authorization.split(' ')[1]}`
        }
      });
      
      dados = response.data;
    } else {
      const { data, error } = await supabase
        .from('produtos_supermercados')
        .select('*')
        .gte('data_coleta', dataInicioISO)
        .lte('data_coleta', dataFimISO);
      
      if (error) {
        return res.status(500).json({ message: 'Erro ao buscar dados para estatísticas', error: error.message });
      }
      
      dados = data;
    }
    
    // Filtrar por CNPJ, se especificado
    if (cnpj) {
      dados = dados.filter(item => item.cnpj_supermercado === cnpj);
    }
    
    // Agrupar por categoria de produto
    const categorias = {};
    
    dados.forEach(registro => {
      const categoria = registro.categoria_produto || 'Outros';
      if (!categorias[categoria]) {
        categorias[categoria] = {
          quantidade: 0,
          precos: []
        };
      }
      categorias[categoria].quantidade++;
      categorias[categoria].precos.push(parseFloat(registro.preco_produto));
    });
    
    // Calcular estatísticas por categoria
    const estatisticasCategorias = Object.keys(categorias).map(categoria => {
      const dadosCategoria = categorias[categoria];
      const precos = dadosCategoria.precos;
      
      return {
        categoria,
        quantidade_produtos: dadosCategoria.quantidade,
        preco_medio: precos.length > 0 ? 
          (precos.reduce((sum, p) => sum + p, 0) / precos.length).toFixed(2) : 
          null,
        preco_minimo: precos.length > 0 ? Math.min(...precos).toFixed(2) : null,
        preco_maximo: precos.length > 0 ? Math.max(...precos).toFixed(2) : null
      };
    });
    
    // Ordenar por quantidade de produtos
    estatisticasCategorias.sort((a, b) => b.quantidade_produtos - a.quantidade_produtos);
    
    // Agrupar por supermercado
    const supermercados = {};
    
    dados.forEach(registro => {
      const cnpj = registro.cnpj_supermercado;
      if (!supermercados[cnpj]) {
        const mercado = MERCADOS.find(m => m.cnpj === cnpj);
        supermercados[cnpj] = {
          nome: mercado ? mercado.nome : 'Desconhecido',
          categoria: mercado ? mercado.categoria : '',
          cidade: mercado ? mercado.cidade : '',
          precos: []
        };
      }
      supermercados[cnpj].precos.push(parseFloat(registro.preco_produto));
    });
    
    // Calcular estatísticas por supermercado
    const estatisticasSupermercados = Object.keys(supermercados).map(cnpj => {
      const dadosSupermercado = supermercados[cnpj];
      const precos = dadosSupermercado.precos;
      
      return {
        cnpj,
        nome: dadosSupermercado.nome,
        categoria: dadosSupermercado.categoria,
        cidade: dadosSupermercado.cidade,
        quantidade_produtos: precos.length,
        preco_medio: precos.length > 0 ? 
          (precos.reduce((sum, p) => sum + p, 0) / precos.length).toFixed(2) : 
          null,
        preco_minimo: precos.length > 0 ? Math.min(...precos).toFixed(2) : null,
        preco_maximo: precos.length > 0 ? Math.max(...precos).toFixed(2) : null
      };
    });
    
    // Ordenar por preço médio
    estatisticasSupermercados.sort((a, b) => parseFloat(a.preco_medio) - parseFloat(b.preco_medio));
    
    res.json({
      periodo: {
        inicio: dataInicioISO,
        fim: dataFimISO,
        dias: periodo
      },
      categorias: estatisticasCategorias,
      supermercados: estatisticasSupermercados,
      total_registros: dados.length
    });
  } catch (error) {
    res.status(500).json({ message: 'Erro ao gerar estatísticas', error: error.message });
  }
});

// Função para coletar dados (adaptada do código original)
async function coletarDados() {
  logger.info(`Iniciando coleta de dados - Versão ${VERSAO_SCRIPT}`);
  
  const dt = new Date();
  const data_coleta = dt.toISOString();
  
  // Limita concorrência entre supermercados
  const limiteMercados = pLimit(CONCORRENCIA_MERCADOS);
  
  const promessasMercados = MERCADOS.map(mercado =>
    limiteMercados(() => coletarDadosMercado(mercado, data_coleta))
  );
  
  const resultados = await Promise.all(promessasMercados);
  
  // Log final com resumo
  logger.info('=== RESUMO DA COLETA ===');
  let totalNovosRegistros = 0;
  
  for (const resultado of resultados) {
    totalNovosRegistros += resultado.novos_registros;
    logger.info(`${resultado.mercado} (${resultado.cnpj}): ${resultado.novos_registros} novos registros`);
  }
  
  logger.info(`TOTAL GERAL: ${totalNovosRegistros} novos registros`);
  logger.info('Coleta finalizada com sucesso!');
  
  return resultados;
}

// Função para coletar dados de um mercado específico
async function coletarDadosMercado(mercado, data_coleta) {
  logger.info(`Iniciando coleta para ${mercado.nome} (${mercado.cnpj})`);
  
  const { nome, cnpj, categoria, cidade } = mercado;
  
  // Limita concorrência para este supermercado
  const limiteProdutos = pLimit(CONCORRENCIA_PRODUTOS);
  
  const promessasProdutos = NOMES_PRODUTOS.map(produto =>
    limiteProdutos(() => consultarProduto(produto, mercado, data_coleta))
  );
  
  const resultados = await Promise.all(promessasProdutos);
  
  // Contar novos registros
  const novosRegistros = resultados.reduce((total, resultado) => total + resultado.novos_registros, 0);
  
  logger.info(`Coleta finalizada para ${mercado.nome}. Novos registros: ${novosRegistros}`);
  
  return {
    mercado: mercado.nome,
    cnpj: mercado.cnpj,
    novos_registros: novosRegistros
  };
}

// Função para consultar um produto em um mercado
async function consultarProduto(produto, mercado, data_coleta) {
  const { nome, cnpj, categoria, cidade } = mercado;
  
  let pagina = 1, totalPaginas = 1, sucessoAlguma = false;
  let novosRegistros = 0;
  
  do {
    const requestBody = {
      produto: { descricao: produto.toUpperCase() },
      estabelecimento: { individual: { cnpj } },
      dias: DIAS_PESQUISA,
      pagina,
      registrosPorPagina: REGISTROS_POR_PAGINA
    };
    
    const fetchWithRetry = () => axios.post(ECONOMIZA_ALAGOAS_API_URL, requestBody, {
      headers: {
        'AppToken': ECONOMIZA_ALAGOAS_TOKEN,
        'Content-Type': 'application/json'
      },
      timeout: 50000
    });
    
    let response;
    try {
      response = await retryAsync(fetchWithRetry, cnpj, produto, RETRY_MAX, RETRY_BASE_MS);
      sucessoAlguma = true;
      
      const conteudo = Array.isArray(response.data?.conteudo) ? response.data.conteudo : [];
      totalPaginas = response.data?.totalPaginas ?? 1;
      
      // Processar e salvar no Supabase
      for (const item of conteudo) {
        const produtoInfo = item.produto || {};
        const venda = produtoInfo.venda || {};
        const nomeNormalizado = normalizarTexto(produtoInfo.descricao);
        const unidade = produtoInfo.unidadeMedida || '';
        const codBarras = produtoInfo.gtin || '';
        const id_produto = gerarIdProduto(produtoInfo.descricao, unidade, codBarras);
        
        const registro = {
          nome_supermercado: nome,
          cnpj_supermercado: cnpj,
          categoria_supermercado: categoria || "",
          cidade_supermercado: cidade || "",
          nome_produto: produtoInfo.descricao || '',
          nome_produto_normalizado: nomeNormalizado,
          id_produto,
          categoria_produto: produtoInfo.categoria || "",
          preco_produto: venda.valorVenda || '',
          unidade_medida: unidade,
          data_ultima_venda: venda.dataVenda || '',
          data_coleta,
          codigo_barras: codBarras,
          origem: "Economiza Alagoas API",
          versao_script: VERSAO_SCRIPT
        };
        
        registro.id_registro = gerarIdRegistro({
          cnpj_supermercado: cnpj,
          id_produto,
          preco_produto: venda.valorVenda || '',
          data_ultima_venda: venda.dataVenda || '',
          data_coleta
        });
        
        // Verificar se o registro já existe
        const { data: existingRecord, error: fetchError } = await supabase
          .from('produtos_supermercados')
          .select('*')
          .eq('id_registro', registro.id_registro)
          .single();
        
        if (!existingRecord) {
          // Inserir novo registro
          const { error: insertError } = await supabase
            .from('produtos_supermercados')
            .insert([registro]);
          
          if (insertError) {
            logger.error(`Erro ao inserir registro: ${insertError.message}`);
          } else {
            novosRegistros++;
          }
        }
      }
      
      logger.info(`[${new Date().toLocaleString()}] CNPJ ${cnpj} - [${nome}] - Produto "${produto}" - Página ${pagina}/${totalPaginas} coletada. Itens: ${conteudo.length}`);
      
    } catch (error) {
      logger.error(`[${new Date().toLocaleString()}] Erro ao consultar "${produto}" para ${nome} (CNPJ ${cnpj}), página ${pagina}: ${error.response?.data || error.message}`);
      break;
    }
    
    pagina++;
    
  } while (pagina <= totalPaginas);
  
  return {
    produto,
    novos_registros: novosRegistros
  };
}

// Função para retry com backoff exponencial
async function retryAsync(fn, cnpj, produto, maxRetries = RETRY_MAX, baseDelay = RETRY_BASE_MS) {
  let attempt = 0;
  let lastErr;
  
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      lastErr = err;
      
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
        logger.warn(`Tentativa ${attempt} para ${cnpj}|${produto} falhou. Retry em ${delay}ms`);
        await new Promise(res => setTimeout(res, delay));
      }
    }
  }
  
  throw lastErr;
}

// Iniciar servidor
app.listen(PORT, () => {
  logger.info(`Servidor rodando na porta ${PORT}`);
});