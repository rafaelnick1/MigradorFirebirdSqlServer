unit Principal;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Classes, System.Variants,
  Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Data.DB,
  FireDAC.Stan.Intf, FireDAC.Stan.Option, FireDAC.Stan.Param,
  FireDAC.Stan.Error, FireDAC.DatS, FireDAC.Phys.Intf,
  FireDAC.DApt.Intf, FireDAC.Stan.Async, FireDAC.DApt,
  FireDAC.Comp.DataSet, FireDAC.Comp.Client, FireDAC.UI.Intf,
  FireDAC.VCLUI.Wait, FireDAC.Comp.UI, FireDAC.Phys,
  FireDAC.Phys.FB, FireDAC.Phys.FBDef, FireDAC.Phys.MSSQL,
  FireDAC.Phys.MSSQLDef, FireDAC.Phys.ODBCBase, Vcl.StdCtrls, Vcl.Buttons,
  FireDAC.Stan.Def, FireDAC.Stan.Pool, System.IniFiles, System.IOUtils, System.StrUtils,
  math;

type
  TMigrador = class(TForm)
    btnMigrar: TBitBtn;
    FDConnFirebird: TFDConnection;
    FDConnSQLServer: TFDConnection;
    MemoLog: TMemo;
    lblTitle: TLabel;
    lblSubtitle: TLabel;
    procedure btnMigrarClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
  private
    QrySQLServer, QryFirebird: TFDQuery;

    function EstabelecerConexoes: Boolean;
    function ConfigurarConexaoFirebird(Ini: TIniFile): Boolean;
    function ConfigurarConexaoSQLServer(Ini: TIniFile): Boolean;

    procedure Limpar;

    procedure MigrarDadosClientes;
    procedure MigrarDadosFornecedores;
    procedure MigrarDadosGrupos;
    procedure MigrarDadosProdutosPorFornecedor;
    procedure MigrarDadosProdutos;
    procedure MigrarDadosProdutosPorEmpresa;

    procedure AtualizarEstoqueProdutos;

    procedure MigrarNF;
    procedure MigrarNFProdutos;
    procedure MigrarVendasXNFE;
    procedure MigrarNFContasReceber;

    procedure MigrarPE;
    procedure MigrarPEProdutos;
    procedure MigrarPEContasReceber;

    procedure MigrarCF;
    procedure MigrarCFProdutos;
    procedure MigrarCFContasReceber;

    procedure MigrarSAT;
    procedure MigrarSATProdutos;
    procedure MigrarVendasXSAT;
    procedure MigrarSATContasReceber;

    procedure MigrarNFCompra;
    procedure MigrarNFContasPagar;
    procedure MigrarNFCompraProdutos;

    procedure LimparTabelaDestino(const NomeTabela: string);
    procedure LogMensagem(const Mensagem: string);
    procedure ExecutarConsultaOrigem(const SQL: string);
    function FormatarDataParaSQL(Data: TDateTime): string;
    function FormatFloatParaSQL(Valor: Double): string;


  public
    { Public declarations }
  end;

var
  Migrador: TMigrador;

implementation

{$R *.dfm}

{ TMigrador }

procedure TMigrador.btnMigrarClick(Sender: TObject);
begin
  MemoLog.Lines.Clear;

  if not EstabelecerConexoes then
    Exit;

  Limpar;

  MigrarDadosClientes;
  MigrarDadosFornecedores;
  MigrarDadosGrupos;
  MigrarDadosProdutosPorFornecedor;
  MigrarDadosProdutos;
  MigrarDadosProdutosPorEmpresa;
  AtualizarEstoqueProdutos;

  MigrarNF;
  MigrarNFProdutos;
  MigrarVendasXNFE;
  MigrarNFContasReceber;

  MigrarPE;
  MigrarPEProdutos;
  MigrarPEContasReceber;

  MigrarCF;
  MigrarCFProdutos;
  MigrarCFContasReceber;

  MigrarVendasXSAT;

  MigrarSAT;
  MigrarSATProdutos;
  MigrarSATContasReceber;

  MigrarNFCompra;
  MigrarNFContasPagar;
  MigrarNFCompraProdutos;
end;

procedure TMigrador.FormCreate(Sender: TObject);
begin
  QrySQLServer := TFDQuery.Create(Self);
  QrySQLServer.Connection := FDConnSQLServer;

  QryFirebird := TFDQuery.Create(Self);
  QryFirebird.Connection := FDConnFirebird;
end;

procedure TMigrador.FormDestroy(Sender: TObject);
begin
  QrySQLServer.Free;
  QryFirebird.Free;
end;

function TMigrador.EstabelecerConexoes: Boolean;
var
  Ini: TIniFile;
  IniPath: string;
begin
  Result := False;

  IniPath := TPath.Combine(ExtractFilePath(ParamStr(0)), 'Config.ini');
  if not FileExists(IniPath) then
  begin
    LogMensagem('Arquivo Config.ini não encontrado!');
    Exit;
  end;

  Ini := TIniFile.Create(IniPath);
  try
    if not ConfigurarConexaoFirebird(Ini) then Exit;
    if not ConfigurarConexaoSQLServer(Ini) then Exit;

    Result := True;
  finally
    Ini.Free;
  end;
end;

function TMigrador.ConfigurarConexaoFirebird(Ini: TIniFile): Boolean;
begin
  Result := False;
  try
    FDConnFirebird.Params.Clear;
    FDConnFirebird.DriverName := 'FB';
    FDConnFirebird.Params.Add('Database=' + Ini.ReadString('Firebird', 'Database', ''));
    FDConnFirebird.Params.Add('User_Name=' + Ini.ReadString('Firebird', 'User_Name', ''));
    FDConnFirebird.Params.Add('Password=' + Ini.ReadString('Firebird', 'Password', ''));
    FDConnFirebird.Params.Add('CharacterSet=' + Ini.ReadString('Firebird', 'CharacterSet', 'WIN1252'));
    FDConnFirebird.Connected := True;
    LogMensagem('Conectado ao Firebird com sucesso!');
    Result := True;
  except
    on E: Exception do
      LogMensagem('Erro ao conectar Firebird: ' + E.Message);
  end;
end;

function TMigrador.ConfigurarConexaoSQLServer(Ini: TIniFile): Boolean;
begin
  Result := False;
  try
    FDConnSQLServer.Params.Clear;
    FDConnSQLServer.DriverName := 'MSSQL';
    FDConnSQLServer.Params.Add('Server=' + Ini.ReadString('SQLServer', 'Server', ''));
    FDConnSQLServer.Params.Add('Database=' + Ini.ReadString('SQLServer', 'Database', ''));
    FDConnSQLServer.Params.Add('OSAuthent=' + Ini.ReadString('SQLServer', 'OSAuthent', 'Yes'));
    FDConnSQLServer.Connected := True;
    LogMensagem('Conectado ao SQL Server com sucesso!');
    Result := True;
  except
    on E: Exception do
      LogMensagem('Erro ao conectar SQL Server: ' + E.Message);
  end;
end;


procedure TMigrador.Limpar;
begin
  LimparTabelaDestino('VENDAS_PRODUTOS');
  LimparTabelaDestino('VENDAS');
  LimparTabelaDestino('CONTAS_RECEBER');


  LimparTabelaDestino('VENDASXCFOP');
  LimparTabelaDestino('VENDASXNFE');

  LimparTabelaDestino('VENDASXSAT');

  LimparTabelaDestino('CLIENTES');

  LimparTabelaDestino('GRUPOS');
  LimparTabelaDestino('FORNECEDORES');

  LimparTabelaDestino('PRODUTOS');
  LimparTabelaDestino('PRODUTOS_POR_EMPRESA');
  LimparTabelaDestino('PRODUTOS_POR_FORNECEDOR');

  LimparTabelaDestino('HISTORICO_ESTOQUE');

  LimparTabelaDestino('CONTAS_PAGAR');
  LimparTabelaDestino('NF_COMPRA');
  LimparTabelaDestino('ORDENS_COMPRA_PRODUTOS');
end;

procedure TMigrador.LimparTabelaDestino(const NomeTabela: string);
begin
  try
    QryFirebird.SQL.Text := 'DELETE FROM ' + NomeTabela;
    QryFirebird.ExecSQL;
    LogMensagem('Tabela ' + NomeTabela + ' limpa com sucesso.');
  except
    on E: Exception do
      LogMensagem('Erro ao limpar tabela ' + NomeTabela + ': ' + E.Message);
  end;
end;

procedure TMigrador.LogMensagem(const Mensagem: string);
begin
  MemoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + ' - ' + Mensagem);
  Application.ProcessMessages;
end;

procedure TMigrador.ExecutarConsultaOrigem(const SQL: string);
begin
  QrySQLServer.Close;
  QrySQLServer.FetchOptions.Mode := fmAll;
  QrySQLServer.FetchOptions.RecsMax := -1;
  QrySQLServer.SQL.Text := SQL;
  QrySQLServer.Open;
end;

function TMigrador.FormatarDataParaSQL(Data: TDateTime): string;
begin
  Result := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Data));
end;

function TMigrador.FormatFloatParaSQL(Valor: Double): string;
var
  Fmt: TFormatSettings;
begin
  Fmt := TFormatSettings.Create('en-US');
  Result := FormatFloat('0.00', RoundTo(Valor, -2), Fmt);
end;





procedure TMigrador.MigrarDadosClientes;
const
  SQL_SELECT =
    'SELECT ' +
    'RIGHT(REPLICATE(''0'', 9) + CAST(PAR.PAR_ID AS VARCHAR(9)), 9) AS CODIGO, ' +
    'LEFT(CAST(PAR.PAR_RAZAOSOCIAL AS VARCHAR(50)), 50) AS NOME, ' +
    'LEFT(CAST(PAR.PAR_NOMEFANTASIA AS VARCHAR(50)), 50) AS FANTASIA, ' +
    'LEFT(CAST(PEN.PEN_LOGRADOURO AS VARCHAR(50)), 50) AS ENDERECO, ' +
    'LEFT(CAST(PEN.PEN_COMPLEMENTO AS VARCHAR(50)), 50) AS COMPLEMENTO, ' +
    'LEFT(CAST(PEN.PEN_BAIRRO AS VARCHAR(30)), 30) AS BAIRRO, ' +
    'LEFT(CAST(MUN.MUN_NOME AS VARCHAR(30)), 30) AS CIDADE, ' +
    'LEFT(PEN.PEN_CEP, 8) AS CEP, ' +
    'EST.EST_UF AS ESTADO, ' +
    'LEFT(CONT1.PCO_CONTATO, 15) AS TELEFONE01, ' +
    'LEFT(CONT2.PCO_CONTATO, 15) AS TELEFONE02, ' +
    'LEFT(PAR.PAR_CONTATO, 30) AS CONTATO, ' +
    'PAR.PAR_DATAABERTURANASCIMENTO AS DATA_NASCIMENTO, ' +
    'CASE WHEN PAR.PAR_TIPOPESSOA = ''F'' THEN ''Física'' ELSE ''Jurídica'' END AS PESSOA, ' +
    'REPLACE(REPLACE(REPLACE(PAR.PAR_CPFCNPJ, ''-'', ''''), ''.'', ''''), ''/'', '''') AS CPF_CNPJ, ' +
    'PAR.PAR_RGINSCRICAOESTADUAL AS RG_INSCRICAO, ' +
    'LEFT(PAR.PAR_OBSERVACAO, 2000) AS OBSERVACOES, ' +
    'PAR.PAR_DATADECADASTRO AS DATA_INC, ' +
    'PAR.PAR_DATAMODIFICACAO AS DATA_ALT, ' +
    'PAR.PAR_DATADECADASTRO AS DATA_CADASTRO, ' +
    '''N'' AS VENDA_CONVENIO, ' +
    '''S'' AS EMITE_CARTA_COBRANCA, ' +
    '''N'' AS EMITE_ALERTA, ' +
    '''N'' AS CASA_PROPRIA, ' +
    'LEFT(PAR.PAR_FILIACAOPAI, 50) AS NOME_PAI, ' +
    'LEFT(PAR.PAR_FILIACAOMAE, 50) AS NOME_MAE, ' +
    'LEFT(PAR.PAR_CONJUGENOME, 50) AS NOME_CONJUGE, ' +
    'LEFT(PAR.PAR_CONJUGECPF, 14) AS CPF_CONJUGE, ' +
    'LEFT(PAR.PAR_CONJUGERG, 20) AS RG_CONJUGE, ' +
    'PAR.PAR_CONJUGEDATANASCIMENTO AS DATA_NASCIMENTO_CONJUGE, ' +
    'LEFT(PAR.PAR_CONJUGELOCALDETRABALHO, 50) AS LOCAL_TRABALHO_CONJUGE, ' +
    'CAST(PAR.PAR_LIMITEDECREDITO AS DECIMAL(18,2)) AS LIMITE_CREDITO, ' +
    '''S'' AS VENDE_VAREJO, ' +
    '''N'' AS VENDE_ATACADO, ' +
    '''N'' AS COBRAR_JUROS, ' +
    'CASE WHEN PAR.PAR_TIPOPESSOA = ''F'' THEN ''S'' ELSE ''N'' END AS CONSUMIDOR_FINAL, ' +
    'LEFT(PEN.PEN_NUMERO, 30) AS NUMERO, ' +
    'CAST(MUN.MUN_ID AS INTEGER) AS IDCIDADE, ' +
    'CASE WHEN PAR.PAR_TIPOPESSOA <> ''F'' AND PAR.PAR_RGINSCRICAOESTADUAL IS NOT NULL AND LTRIM(RTRIM(PAR.PAR_RGINSCRICAOESTADUAL)) <> '''' THEN ''S'' ELSE ''N'' END AS CONTRIBUINTE, ' +
    'CASE WHEN PAR.PAR_IDTIPODEOPERACAOSTATUS = 140 THEN ''S'' ELSE ''N'' END AS STATUS, ' +
    ' '' '' AS MENSAGEM_ALERTA, ' +
    ' ''001'' AS IDGRUPO ' +
    'FROM PARTICIPANTES PAR ' +
    'LEFT JOIN (SELECT PEN_IDPARTICIPANTE, PEN_LOGRADOURO, PEN_COMPLEMENTO, PEN_BAIRRO, PEN_CEP, PEN_IDMUNICIPIO, PEN_NUMERO ' +
    '           FROM (SELECT PEN_IDPARTICIPANTE, PEN_LOGRADOURO, PEN_COMPLEMENTO, PEN_BAIRRO, PEN_CEP, PEN_IDMUNICIPIO, PEN_NUMERO, ' +
    '                        ROW_NUMBER() OVER (PARTITION BY PEN_IDPARTICIPANTE ORDER BY PEN_ID DESC) AS RN ' +
    '                 FROM PARTICIPANTESENDERECOS) ranked ' +
    '           WHERE RN = 1) PEN ON PAR.PAR_ID = PEN.PEN_IDPARTICIPANTE ' +
    'LEFT JOIN MUNICIPIOS MUN ON PEN.PEN_IDMUNICIPIO = MUN.MUN_ID ' +
    'LEFT JOIN ESTADOS EST ON MUN.MUN_IDESTADO = EST.EST_ID ' +
    'OUTER APPLY ( ' +
    '  SELECT TOP 1 PCO_CONTATO FROM PARTICIPANTESCONTATOS WHERE PCO_IDPARTICIPANTE = PAR.PAR_ID ORDER BY PCO_ID ' +
    ') CONT1 ' +
    'OUTER APPLY ( ' +
    '  SELECT TOP 1 PCO_CONTATO FROM ( ' +
    '    SELECT PCO_CONTATO, ROW_NUMBER() OVER (ORDER BY PCO_ID) AS RN FROM PARTICIPANTESCONTATOS WHERE PCO_IDPARTICIPANTE = PAR.PAR_ID ' +
    '  ) X WHERE X.RN = 2 ' +
    ') CONT2 ' +
    'WHERE PAR.PAR_ID NOT IN (''1'') ' +
    'AND EXISTS ( ' +
    '  SELECT 1 FROM PARTICIPANTESTIPOSDEOPERACAO PTO  ' +
    '  WHERE PTO.PTO_IDPARTICIPANTE = PAR.PAR_ID  ' +
    '  AND PTO.PTO_IDTIPODEOPERACAO IN (12, 13, 14) ' +
    ')';
begin
  LogMensagem('Iniciando migração de clientes...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CLIENTES (' +
          'CODIGO, NOME, FANTASIA, ENDERECO, COMPLEMENTO, BAIRRO, CIDADE, CEP, ESTADO, TELEFONE01, ' +
          'TELEFONE02, CONTATO, DATA_NASCIMENTO, PESSOA, CPF_CNPJ, RG_INSCRICAO, OBSERVACOES, DATA_INC, DATA_ALT, DATA_CADASTRO, ' +
          'VENDA_CONVENIO, EMITE_CARTA_COBRANCA, EMITE_ALERTA, CASA_PROPRIA, NOME_PAI, NOME_MAE, NOME_CONJUGE, CPF_CONJUGE, RG_CONJUGE, DATA_NASCIMENTO_CONJUGE, ' +
          'LOCAL_TRABALHO_CONJUGE, LIMITE_CREDITO, VENDE_VAREJO, VENDE_ATACADO, COBRAR_JUROS, CONSUMIDOR_FINAL, NUMERO, IDCIDADE, CONTRIBUINTE, STATUS, ' +
          'IDGRUPO) ' +
          'VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FANTASIA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('ENDERECO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COMPLEMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('BAIRRO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CIDADE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CEP').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('ESTADO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TELEFONE01').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TELEFONE02').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTATO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_NASCIMENTO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('PESSOA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CPF_CNPJ').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('RG_INSCRICAO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('OBSERVACOES').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_INC').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_ALT').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CADASTRO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDA_CONVENIO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('EMITE_CARTA_COBRANCA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('EMITE_ALERTA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CASA_PROPRIA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME_PAI').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME_MAE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME_CONJUGE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CPF_CONJUGE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('RG_CONJUGE').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_NASCIMENTO_CONJUGE').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('LOCAL_TRABALHO_CONJUGE').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('LIMITE_CREDITO').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDE_VAREJO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDE_ATACADO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COBRAR_JUROS').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONSUMIDOR_FINAL').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NUMERO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('IDCIDADE').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTRIBUINTE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('STATUS').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDGRUPO').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir cliente ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de clientes concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de clientes: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarDadosFornecedores;
const
  SQL_SELECT =
    'SELECT ' +
    'RIGHT(REPLICATE(''0'', 6) + CAST(PAR.PAR_ID AS VARCHAR(6)), 6) AS CODIGO, ' +
    'LEFT(CAST(PAR.PAR_RAZAOSOCIAL AS VARCHAR(50)), 50) AS NOME, ' +
    'NULL AS CODIGO_ANTERIOR, ' +
    'LEFT(CAST(PAR.PAR_NOMEFANTASIA AS VARCHAR(50)), 50) AS FANTASIA, ' +
    'LEFT(CAST(PEN.PEN_LOGRADOURO AS VARCHAR(50)), 50) AS ENDERECO, ' +
    'LEFT(CAST(PEN.PEN_COMPLEMENTO AS VARCHAR(50)), 50) AS COMPLEMENTO, ' +
    'LEFT(CAST(PEN.PEN_BAIRRO AS VARCHAR(30)), 30) AS BAIRRO, ' +
    'LEFT(CAST(MUN.MUN_NOME AS VARCHAR(30)), 30) AS CIDADE, ' +
    'LEFT(PEN.PEN_CEP, 9) AS CEP, ' +
    'NULL AS CXPOSTAL, ' +
    'EST.EST_UF AS ESTADO, ' +
    'CASE WHEN PAR.PAR_IDTIPODEOPERACAOSTATUS = 140 THEN ''S'' ELSE ''N'' END AS STATUS, ' +
    'LEFT(CONT1.PCO_CONTATO, 15) AS TELEFONE01, ' +
    'LEFT(CONT2.PCO_CONTATO, 15) AS TELEFONE02, ' +
    'NULL AS FAX, ' +
    'NULL AS CELULAR, ' +
    'LEFT(PAR.PAR_CONTATO, 200) AS CONTATO, ' +
    'NULL AS RAMAL, ' +
    'NULL AS HOMEPAGE, ' +
    'NULL AS EMAIL, ' +
    'LEFT(PAR.PAR_OBSERVACAO, 500) AS OBSERVACOES, ' +
    'PAR.PAR_DATAABERTURANASCIMENTO AS DATA_NASCIMENTO, ' +
    'CASE WHEN PAR.PAR_TIPOPESSOA = ''F'' THEN ''Física'' ELSE ''Jurídica'' END AS PESSOA, ' +
    'NULL AS SEXO, ' +
    'NULL AS ESTADO_CIVIL, ' +
    'PAR.PAR_RGINSCRICAOESTADUAL AS RG_INSCRICAO, ' +
    'REPLACE(REPLACE(REPLACE(PAR.PAR_CPFCNPJ, ''-'', ''''), ''.'', ''''), ''/'', '''') AS CPF_CNPJ, ' +
    'PAR.PAR_DATADECADASTRO AS DATA_INC, ' +
    'NULL AS TRANSPORTADORA, ' +
    'NULL AS USU_INC, ' +
    'PAR.PAR_DATAMODIFICACAO AS DATA_ALT, ' +
    'NULL AS USU_ALT, ' +
    'NULL AS PRAZO_ENTREGA, ' +
    'NULL AS BANCO, ' +
    'NULL AS AGENCIA, ' +
    'NULL AS CONTA, ' +
    'CAST(0 AS DECIMAL(18,2)) AS VALOR_MINIMO_COMPRA, ' +
    'CAST(MUN.MUN_ID AS INTEGER) AS IDCIDADE, ' +
    'LEFT(PEN.PEN_NUMERO, 10) AS NUMERO ' +
    'FROM PARTICIPANTES PAR ' +
    'LEFT JOIN PARTICIPANTESENDERECOS PEN ON PAR.PAR_ID = PEN.PEN_IDPARTICIPANTE ' +
    'LEFT JOIN MUNICIPIOS MUN ON PEN.PEN_IDMUNICIPIO = MUN.MUN_ID ' +
    'LEFT JOIN ESTADOS EST ON MUN.MUN_IDESTADO = EST.EST_ID ' +
    'LEFT JOIN PARTICIPANTESTIPOSDEOPERACAO PTO ON PTO.PTO_IDPARTICIPANTE = PAR.PAR_ID ' +
    'OUTER APPLY ( ' +
    '  SELECT TOP 1 PCO_CONTATO FROM PARTICIPANTESCONTATOS WHERE PCO_IDPARTICIPANTE = PAR.PAR_ID ORDER BY PCO_ID ' +
    ') CONT1 ' +
    'OUTER APPLY ( ' +
    '  SELECT TOP 1 PCO_CONTATO FROM ( ' +
    '    SELECT PCO_CONTATO, ROW_NUMBER() OVER (ORDER BY PCO_ID) AS RN FROM PARTICIPANTESCONTATOS WHERE PCO_IDPARTICIPANTE = PAR.PAR_ID ' +
    '  ) X WHERE X.RN = 2 ' +
    ') CONT2 ' +
    'WHERE PAR.PAR_ID NOT IN (''1'', ''3'') ' +
    'AND PTO.PTO_IDTIPODEOPERACAO = 13';
begin
  LogMensagem('Iniciando migração de fornecedores...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO FORNECEDORES (' +
          'CODIGO, NOME, CODIGO_ANTERIOR, FANTASIA, ENDERECO, COMPLEMENTO, BAIRRO, CIDADE, CEP, CXPOSTAL, ' +
          'ESTADO, STATUS, TELEFONE01, TELEFONE02, FAX, CELULAR, CONTATO, RAMAL, HOMEPAGE, EMAIL, OBSERVACOES, ' +
          'DATA_NASCIMENTO, PESSOA, SEXO, ESTADO_CIVIL, RG_INSCRICAO, CPF_CNPJ, DATA_INC, TRANSPORTADORA, USU_INC, DATA_ALT, USU_ALT, ' +
          'PRAZO_ENTREGA, BANCO, AGENCIA, CONTA, VALOR_MINIMO_COMPRA, IDCIDADE, NUMERO) ' +
          'VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME').AsString) + ', ' +
          'NULL, ' +
          QuotedStr(QrySQLServer.FieldByName('FANTASIA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('ENDERECO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COMPLEMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('BAIRRO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CIDADE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CEP').AsString) + ', ' +
          'NULL, ' +
          QuotedStr(QrySQLServer.FieldByName('ESTADO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('STATUS').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TELEFONE01').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TELEFONE02').AsString) + ', ' +
          'NULL, ' +
          'NULL, ' +
          QuotedStr(QrySQLServer.FieldByName('CONTATO').AsString) + ', ' +
          'NULL, ' +
          'NULL, ' +
          'NULL, ' +
          QuotedStr(QrySQLServer.FieldByName('OBSERVACOES').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_NASCIMENTO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('PESSOA').AsString) + ', ' +
          'NULL, ' +
          'NULL, ' +
          QuotedStr(QrySQLServer.FieldByName('RG_INSCRICAO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CPF_CNPJ').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_INC').AsDateTime) + ', ' +
          'NULL, ' +
          'NULL, ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_ALT').AsDateTime) + ', ' +
          'NULL, ' +
          'NULL, ' +
          'NULL, ' +
          'NULL, ' +
          'NULL, ' +
          '0, ' +
          IntToStr(QrySQLServer.FieldByName('IDCIDADE').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NUMERO').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir fornecedor ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de fornecedores concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de fornecedores: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarDadosGrupos;
const
  SQL_SELECT =
    'SELECT ' +
    'RIGHT(REPLICATE(''0'', 4) + CAST(IGS_ID AS VARCHAR(4)), 4) AS GRUPO, ' +
    'LEFT(IGS_NOME, 50) AS NOME ' +
    'FROM ITENSGRUPOSUBGRUPO';
begin
  LogMensagem('Iniciando migração de grupos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO GRUPOS (GRUPO, NOME) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('GRUPO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NOME').AsString) + ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir grupo ' + QrySQLServer.FieldByName('GRUPO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de grupos concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de grupos: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarDadosProdutosPorFornecedor;
const
  SQL_SELECT =
    'WITH CTE AS (' +
    '  SELECT ' +
    '    RIGHT(REPLICATE(''0'', 6) + CAST(ICO_IDITEM AS VARCHAR(6)), 6) AS CODIGO, ' +
    '    RIGHT(REPLICATE(''0'', 6) + CAST(ICO_IDPARTICIPANTE AS VARCHAR(6)), 6) AS FORNECEDOR, ' +
    '    ''S'' AS PADRAO, ' +
    '    LEFT(ICO_CODIGOPARTICIPANTE, 60) AS COD_PROD_FOR, ' +
    '    ROW_NUMBER() OVER (PARTITION BY ICO_IDITEM, ICO_IDPARTICIPANTE ORDER BY ICO_DATAMODIFICACAO DESC) AS RN ' +
    '  FROM ITENSPARTICIPANTES' +
    ') ' +
    'SELECT CODIGO, FORNECEDOR, PADRAO, COD_PROD_FOR ' +
    'FROM CTE WHERE RN = 1';
begin
  LogMensagem('Iniciando migração de produtos por fornecedor...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO PRODUTOS_POR_FORNECEDOR (CODIGO, FORNECEDOR, PADRAO, COD_PROD_FOR) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORNECEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('PADRAO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COD_PROD_FOR').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto por fornecedor ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos por fornecedor concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos por fornecedor: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarDadosProdutos;
const
  SQL_SELECT =
    'SELECT ' +
    '  RIGHT(REPLICATE(''0'', 6) + CAST(I.ITE_ID AS VARCHAR(6)), 6) AS CODIGO, ' +
    '  LEFT(TRIM(I.ITE_DESCRICAOPRINCIPAL), 100) AS DESCRICAO, ' +
    '  LEFT(I.ITE_CODIGOGTIN, 13) AS CODIGO_BARRA, ' +
    '  LEFT(I.ITE_CODIGOINTERNO, 30) AS CODIGO_ANTERIOR, ' +
    '  LEFT(CAST(I.ITE_IDORIGEM AS VARCHAR(1)), 1) AS CLAS_ORIGEM, ' +
    '  ''201'' AS CLAS_DESPESA, ' +
    '  ''000001'' AS COR, ' +
    '  ''0001'' AS MARCA, ' +
    '  ''0001'' AS DPTO_PRODUTO, ' +
    '  RIGHT(REPLICATE(''0'', 4) + CAST(G.IGS_ID AS VARCHAR(4)), 4) AS GRUPO, ' +
    '  CASE ' +
    '    WHEN U.IUN_SIGLA = ''PAR'' THEN ''PR'' ' +
    '    WHEN U.IUN_SIGLA = ''M'' THEN ''ML'' ' +
    '    WHEN U.IUN_SIGLA IS NULL OR LTRIM(RTRIM(U.IUN_SIGLA)) = '''' THEN ''UN'' ' +
    '    ELSE LEFT(U.IUN_SIGLA, 4) ' +
    '  END AS UMEDIDA, ' +
    '  CASE WHEN I.ITE_IDTIPODEOPERACAOSTATUS = 140 THEN ''S'' ELSE ''N'' END AS STATUS, ' +
    '  ''N'' AS MONTAGEM, ' +
    '  ''S'' AS ENTREGA, ' +
    '  ''S'' AS BAIXA_ESTOQUE, ' +
    '  ''P'' AS TIPO, ' +
    '  ''N'' AS COMPOSICAO, ' +
    '  I.ITE_DATAMODIFICACAO AS DATA_ALT, ' +
    '  CAST(1 AS NUMERIC(18,4)) AS QUANTIDADE_EMBALAGEM, ' +
    '  LEFT(NC.NCM_NCM, 8) AS NCM ' +
    'FROM ITENS I ' +
    'LEFT JOIN ITENSGRUPOSUBGRUPO G ON G.IGS_ID = I.ITE_IDGRUPO ' +
    'LEFT JOIN ITENSUNIDADES U ON U.IUN_ID = I.ITE_IDUNIDADEVENDA ' +
    'LEFT JOIN ITENSNCM NC ON NC.NCM_ID = I.ITE_IDNCM';
begin
  LogMensagem('Iniciando migração de produtos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO PRODUTOS (' +
          'CODIGO, DESCRICAO, CODIGO_BARRA, CODIGO_ANTERIOR, CLAS_ORIGEM, CLAS_DESPESA, COR, MARCA, DPTO_PRODUTO, ' +
          'GRUPO, UMEDIDA, STATUS, MONTAGEM, ENTREGA, BAIXA_ESTOQUE, TIPO, COMPOSICAO, DATA_ALT, QUANTIDADE_EMBALAGEM, NCM) ' +
          'VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('DESCRICAO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO_BARRA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO_ANTERIOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLAS_ORIGEM').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLAS_DESPESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('MARCA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('DPTO_PRODUTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('GRUPO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('UMEDIDA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('STATUS').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('MONTAGEM').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('ENTREGA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('BAIXA_ESTOQUE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('COMPOSICAO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_ALT').AsDateTime) + ', ' +
          QrySQLServer.FieldByName('QUANTIDADE_EMBALAGEM').AsString + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NCM').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarDadosProdutosPorEmpresa;
const
  SQL_SELECT =
    'SELECT ' +
    '  RIGHT(REPLICATE(''0'', 6) + CAST(I.ITE_ID AS VARCHAR(6)), 6) AS CODIGO, ' +
    '  ''001'' AS SIGLA_EMPRESA, ' +
    '  1 AS ICMS_TABELA, ' +
    '  ISNULL(T.TBI_CUSTO, 0) AS VALOR_CUSTO_BRUTO, ' +
    '  ISNULL(T.TBI_CUSTO, 0) AS VALOR_CUSTO, ' +
    '  ISNULL(T.TBI_MARGEM, 0) AS MARGEM_LUCRO, ' +
    '  ISNULL(T.TBI_VALOR, 0) AS VALOR_VENDA, ' +
    '  ISNULL(T.TBI_COMISSAO, 0) AS COMISSAO, ' +
    '  0 AS ESTOQUE, ' +
    '  ''N'' AS VENDA_ATACADO, ' +
    '  ''S'' AS VENDA_VAREJO, ' +
    '  ''001'' AS IDESTOQUE ' +
    'FROM ITENS I ' +
    'LEFT JOIN TABELADEPRECOSITENS T ON T.TBI_IDITEM = I.ITE_ID ' +
    'WHERE T.TBI_IDTABELADEPRECOS = 1';
begin
  LogMensagem('Iniciando migração de produtos por empresa...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO PRODUTOS_POR_EMPRESA (' +
          'CODIGO, SIGLA_EMPRESA, ICMS_TABELA, VALOR_CUSTO_BRUTO, VALOR_CUSTO, ' +
          'MARGEM_LUCRO, VALOR_VENDA, COMISSAO, ESTOQUE, VENDA_ATACADO, ' +
          'VENDA_VAREJO, IDESTOQUE) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('ICMS_TABELA').AsInteger) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_CUSTO_BRUTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_CUSTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('MARGEM_LUCRO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_VENDA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('COMISSAO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('ESTOQUE').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDA_ATACADO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDA_VAREJO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDESTOQUE').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto por empresa ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos por empresa concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos por empresa: ' + E.Message);
  end;
end;

procedure TMigrador.AtualizarEstoqueProdutos;
const
  SQL_ESTOQUE =
    'SELECT ' +
    '  RIGHT(REPLICATE(''0'', 6) + CAST(EMV_IDITEM AS VARCHAR(6)), 6) AS CODIGO, ' +
    '  CASE WHEN SUM(EMV_QUANTIDADE) < 0 THEN 0 ELSE SUM(EMV_QUANTIDADE) END AS ESTOQUE ' +
    'FROM ESTOQUEMOVIMENTACAO ' +
    'WHERE EMV_IDITEM IS NOT NULL ' +
    'GROUP BY EMV_IDITEM';
begin
  LogMensagem('Iniciando atualização de estoque dos produtos...');

  try
    QryFirebird.SQL.Text := 'SET GENERATOR GEN_HISTORICO_ESTOQUE_ID TO 0';
    QryFirebird.ExecSQL;

    ExecutarConsultaOrigem(SQL_ESTOQUE);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'UPDATE PRODUTOS_POR_EMPRESA ' +
          'SET ESTOQUE = ' + FormatFloatParaSQL(QrySQLServer.FieldByName('ESTOQUE').AsFloat) + ' ' +
          'WHERE CODIGO = ' + QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ' ' +
          'AND SIGLA_EMPRESA = ''001''';

        QryFirebird.ExecSQL;

        QryFirebird.SQL.Text :=
          'INSERT INTO HISTORICO_ESTOQUE (' +
          'CODIGO, SIGLA_EMPRESA, TIPO_COMPRAS_VENDAS, TIPO_ENTRADA_SAIDA, ' +
          'TIPO_DOCUMENTO, DATA_DOCUMENTO, NRO_DOCUMENTO, EMPRESA_DOCUMENTO, ' +
          'CLIENTE_FORNECEDOR, PRODUTO, MOTIVO_ESTOQUE, QUANTIDADE) ' +
          'VALUES (' +
          'GEN_ID(GEN_HISTORICO_ESTOQUE_ID, 1), ' +
          '''001'', ' +
          '''C'', ' +
          '''E'', ' +
          '''AE'', ' +
          FormatarDataParaSQL(Now) + ', ' +
          '''0'', ' +
          '''001'', ' +
          '''0'', ' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          '''Migração'', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('ESTOQUE').AsFloat) +
          ')';

        QryFirebird.ExecSQL;

      except
        on E: Exception do
          LogMensagem('Erro ao atualizar estoque do produto ' +
                     QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Atualização de estoque concluída. Total de produtos atualizados: ' +
                IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante atualização de estoque: ' + E.Message);
  end;
end;





procedure TMigrador.MigrarNF;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''NF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  ''NF'' AS TIPO_DOCUMENTO, ' +
    '  ''001'' as SIGLA_EMPRESA, ' +
    '  LEFT(cdp.CDP_NOME, 15) AS FORMA_PAGAMENTO, ' +
    '  d.DOC_DATAEMISSAO AS DATA_EMISSAO, ' +
    '  ''000000001'' AS VENDEDOR, ' +
    '  RIGHT(REPLICATE(''0'', 9) + CAST(par.PAR_ID AS VARCHAR(9)), 9) AS CLIENTE, ' +
    '  ''NF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CONTA_RECEBER, ' +
    '  ROUND(SUM(di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL), 2) AS VALOR_PRODUTOS, ' +
    '  ROUND(SUM(ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_DESCONTO, ' +
    '  ROUND(SUM((di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL) - ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_TOTAL, ' +
    '  ''VD'' as IDOPERACAO, ' +
    '  CASE ' +
    '    WHEN d.DOC_IDTIPODEOPERACAOSTATUS IN (31, 32) THEN ''N'' ' +
    '    ELSE ''S'' ' +
    '  END AS CANCELADO_CUPOM, ' +
    '  d.DOC_DATACANCELAMENTONFE AS DATA_CANCELAMENTO ' +
    'FROM DOCUMENTOS d ' +
    'JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    'LEFT JOIN CONDICOESDEPAGAMENTO cdp ON cdp.CDP_ID = d.DOC_IDCONDICOESDEPAGAMENTO ' +
    'LEFT JOIN PARTICIPANTES par ON par.PAR_ID = d.DOC_IDPARTICIPANTE ' +
    'WHERE d.DOC_IDTIPODEOPERACAO IN (3, 307) ' + //NF
    '  AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    'GROUP BY ' +
    '  d.DOC_ID, d.DOC_NRDOCUMENTO, d.DOC_DATAEMISSAO, d.DOC_CHAVENFE, ' +
    '  d.DOC_DATACANCELAMENTONFE, ' +
    '  cdp.CDP_NOME, par.PAR_ID, d.DOC_IDTIPODEOPERACAOSTATUS';
var
  CanceladoCupom, CanceladoMensagem: string;
begin
  LogMensagem('Iniciando migração de vendas...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        CanceladoCupom := QrySQLServer.FieldByName('CANCELADO_CUPOM').AsString;
        CanceladoMensagem := IfThen(CanceladoCupom = 'S', 'CANCELADO', '');

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS (' +
          'CODIGO, TIPO_DOCUMENTO, SIGLA_EMPRESA, FORMA_PAGAMENTO, DATA_EMISSAO, ' +
          'VENDEDOR, CLIENTE, CONTA_RECEBER, VALOR_PRODUTOS, VALOR_DESCONTO, ' +
          'VALOR_TOTAL, IDOPERACAO, CANCELADO_CUPOM, CANCELADO_DATA, CANCELADO_MENSAGEM) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORMA_PAGAMENTO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTA_RECEBER').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_PRODUTOS').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDOPERACAO').AsString) + ', ' +
          QuotedStr(CanceladoCupom) + ', ' +
          IfThen(
            not QrySQLServer.FieldByName('DATA_CANCELAMENTO').IsNull,
            FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CANCELAMENTO').AsDateTime),
            'NULL'
          ) + ', ' +
          QuotedStr(CanceladoMensagem) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir venda ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de vendas concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de vendas: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarNFProdutos;
const
  SQL_SELECT =
    'WITH RankedItens AS ( ' +
    '    SELECT ' +
    '        ''NF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '        ''NF'' AS TIPO_DOCUMENTO, ' +
    '        di.DIT_NRITEM AS ITEM_VENDA, ' +
    '        ''001'' AS SIGLA_EMPRESA, ' +
    '        RIGHT(REPLICATE(''0'', 6) + CAST(di.DIT_IDITEM AS VARCHAR(6)), 6) AS PRODUTO, ' +
    '        di.DIT_QTDCOMERCIAL, ' +
    '        di.DIT_VALORUNITARIOCOMERCIAL, ' +
    '        di.DIT_VALORDESCONTO, ' +
    '        di.DIT_IDTIPODEOPERACAOSTATUS, ' +
    '        ROW_NUMBER() OVER ( ' +
    '            PARTITION BY d.DOC_ID, di.DIT_NRITEM  ' +
    '            ORDER BY CASE WHEN di.DIT_IDTIPODEOPERACAOSTATUS <> 34 THEN 1 ELSE 2 END ' +
    '        ) as RowNum ' +
    '    FROM DOCUMENTOS d ' +
    '    JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    '    WHERE d.DOC_IDTIPODEOPERACAO IN (3, 307) ' + //NF
    '  AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    ') ' +
    'SELECT  ' +
    '    CODIGO,  ' +
    '    TIPO_DOCUMENTO,  ' +
    '    ITEM_VENDA,  ' +
    '    SIGLA_EMPRESA,  ' +
    '    PRODUTO, ' +
    '    CAST(DIT_QTDCOMERCIAL AS DECIMAL(18,3)) AS QUANTIDADE, ' +
    '    CAST(DIT_VALORUNITARIOCOMERCIAL AS DECIMAL(18,4)) AS VALOR_UNITARIO, ' +
    '    CAST(-ISNULL(DIT_VALORDESCONTO, 0) AS DECIMAL(18,2)) AS VALOR_DESCONTO, ' +
    '    CAST((DIT_QTDCOMERCIAL * DIT_VALORUNITARIOCOMERCIAL) AS DECIMAL(18,2)) AS VALOR_TOTAL, ' +
    '    ''N'' AS REALIZAR_ENTREGA,    ' +
    '    ''N'' AS REALIZAR_MONTAGEM, ' +
    '    ''1'' AS IDICMS ' +
    'FROM RankedItens ' +
    'WHERE RowNum = 1 ' +
    'ORDER BY CODIGO, ITEM_VENDA';
begin
  LogMensagem('Iniciando migração de produtos das notas fiscais...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS_PRODUTOS (' +
          'CODIGO, TIPO_DOCUMENTO, ITEM_VENDA, SIGLA_EMPRESA, PRODUTO, QUANTIDADE, ' +
          'VALOR_UNITARIO, VALOR_DESCONTO, VALOR_TOTAL, REALIZAR_ENTREGA, REALIZAR_MONTAGEM, IDICMS) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('PRODUTO').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('QUANTIDADE').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_UNITARIO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('REALIZAR_ENTREGA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('REALIZAR_MONTAGEM').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDICMS').AsString) +
          ')';

        QryFirebird.ExecSQL;

      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto da venda ' + QrySQLServer.FieldByName('CODIGO').AsString +
                     ', item ' + IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos das notas fiscais concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos das notas fiscais: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarNFContasReceber;
const
  SQL_SELECT =
  'SELECT ' +
      '  ''NF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
      '  ROW_NUMBER() OVER (PARTITION BY d.DOC_ID ORDER BY dp.DDU_ID) AS PARCELA, ' +
      '  1 AS SUBPARCELA, ' +
      '  ''001'' AS SIGLA_EMPRESA, ' +
      '  ''N'' AS TIPO_ENTRADA, ' +
      '  RIGHT(REPLICATE(''0'', 9) + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(9)), 9) AS CLIENTE, ' +
      '  dp.DDU_VALORORIGINAL as VALOR, ' +
      '  dp.DDU_VALORORIGINAL as VLR_ORIGINAL, ' +
      '  dp.DDU_DATAEMISSAO as DATA, ' +
      '  dp.DDU_DATAVENCIMENTO as VENCIMENTO, ' +
      '  dpa.DPA_DATAPAGAMENTO as PAGAMENTO, ' +
      '  dp.DDU_VALORJUROS as VLR_JUROS_DIA, ' +
      '  dp.DDU_VALORMULTA as VLR_MULTA, ' +
      '  dp.DDU_VALORDESCONTO as VLR_DESCONTO, ' +
      '  dpa.DPA_VALORPAGO as RECEBIDO, ' +
      '  dp.DDU_VALORTOTALGERAL as VLR_TOTAL ' +
  'FROM DOCUMENTOS d ' +
  'LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
  'LEFT JOIN DOCUMENTOSDUPLICATASPAGAMENTOS dpa ON dpa.DPA_IDDUPLICATAS = dp.DDU_ID ' +
  'WHERE d.DOC_NRDOCUMENTO <> '''' ' +
      '  AND d.DOC_IDTIPODEOPERACAO = 3 ' + //NF
      '  AND dpa.DPA_DATAESTORNO IS NULL ' +  // ignora estornados
      '  AND NOT (dp.DDU_VALORORIGINAL IS NULL AND dp.DDU_DATAEMISSAO IS NULL) ' + // ignora sem valor e sem data
      '  AND dp.DDU_IDTIPODEOPERACAOSTATUS <> 34 ' + // exclui cancelados
  'ORDER BY d.DOC_IDTIPODEOPERACAO, PARCELA';
begin
  LogMensagem('Iniciando migração de Contas a Receber...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CONTAS_RECEBER (' +
          'CODIGO, PARCELA, SUBPARCELA, SIGLA_EMPRESA, TIPO_ENTRADA, CLIENTE, ' +
          'VALOR, DATA, VENCIMENTO, PAGAMENTO, VLR_ORIGINAL, VLR_JUROS_DIA, ' +
          'VLR_MULTA, VLR_DESCONTO, RECEBIDO, VLR_TOTAL) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('PARCELA').AsInteger) + ', ' +
          IntToStr(QrySQLServer.FieldByName('SUBPARCELA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_ENTRADA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR').AsFloat) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('VENCIMENTO').AsDateTime) + ', ' +
          IfThen(not QrySQLServer.FieldByName('PAGAMENTO').IsNull,
                 FormatarDataParaSQL(QrySQLServer.FieldByName('PAGAMENTO').AsDateTime),
                 'NULL') + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_ORIGINAL').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_JUROS_DIA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_MULTA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('RECEBIDO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_TOTAL').AsFloat) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir conta a receber do doc ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de Contas a Receber concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de Contas a Receber: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarVendasXNFE;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''NF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  d.DOC_CHAVENFE, ' +
    '  d.DOC_DATAENVIONFE, ' +
    '  d.DOC_DATACANCELAMENTONFE ' +
    'FROM DOCUMENTOS d ' +
    'WHERE d.DOC_IDTIPODEOPERACAO IN (3, 307)';
begin
  LogMensagem('Iniciando migração de VENDASXNFE...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        var Codigo := QrySQLServer.FieldByName('CODIGO').AsString;
        var Chave  := QrySQLServer.FieldByName('DOC_CHAVENFE').AsString;

        var Protocolo, ProtocoloCancelamento, DataSQL: string;
        Protocolo := 'NULL';
        ProtocoloCancelamento := 'NULL';
        DataSQL := 'NULL';

        // Se tiver data de envio
        if not QrySQLServer.FieldByName('DOC_DATAENVIONFE').IsNull then
        begin
          Protocolo := QuotedStr('ENVIADA');
          DataSQL   := FormatarDataParaSQL(QrySQLServer.FieldByName('DOC_DATAENVIONFE').AsDateTime);
        end;

        // Se tiver data de cancelamento
        if not QrySQLServer.FieldByName('DOC_DATACANCELAMENTONFE').IsNull then
        begin
          ProtocoloCancelamento := QuotedStr('CANCELADA');
          DataSQL := FormatarDataParaSQL(QrySQLServer.FieldByName('DOC_DATACANCELAMENTONFE').AsDateTime);
        end;

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDASXNFE (CODIGO, PROTOCOLO, CHAVE, DATA, PROTOCOLO_CANCELAMENTO) VALUES (' +
          QuotedStr(Codigo) + ', ' +
          Protocolo + ', ' +
          QuotedStr(Chave) + ', ' +
          DataSQL + ', ' +
          ProtocoloCancelamento +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir VENDASXNFE ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de VENDASXNFE concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de VENDASXNFE: ' + E.Message);
  end;
end;





procedure TMigrador.MigrarPE;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''PE'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  ''PE'' AS TIPO_DOCUMENTO, ' +
    '  ''001'' as SIGLA_EMPRESA, ' +
    '  LEFT(cdp.CDP_NOME, 15) AS FORMA_PAGAMENTO, ' +
    '  d.DOC_DATAEMISSAO AS DATA_EMISSAO, ' +
    '  ''000000001'' AS VENDEDOR, ' +
    '  RIGHT(REPLICATE(''0'', 9) + CAST(par.PAR_ID AS VARCHAR(9)), 9) AS CLIENTE, ' +
    '  ''PE'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CONTA_RECEBER, ' +
    '  ROUND(SUM(di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL), 2) AS VALOR_PRODUTOS, ' +
    '  ROUND(SUM(ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_DESCONTO, ' +
    '  ROUND(SUM((di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL) - ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_TOTAL, ' +
    '  ''VD'' as IDOPERACAO, ' +
    '  CASE ' +
    '    WHEN d.DOC_IDTIPODEOPERACAOSTATUS IN (31, 32) THEN ''N'' ' +
    '    ELSE ''S'' ' +
    '  END AS CANCELADO_CUPOM, ' +
    '  d.DOC_DATACANCELAMENTONFE AS DATA_CANCELAMENTO ' +
    'FROM DOCUMENTOS d ' +
    'JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    'LEFT JOIN CONDICOESDEPAGAMENTO cdp ON cdp.CDP_ID = d.DOC_IDCONDICOESDEPAGAMENTO ' +
    'LEFT JOIN PARTICIPANTES par ON par.PAR_ID = d.DOC_IDPARTICIPANTE ' +
    'WHERE d.DOC_IDTIPODEOPERACAO = 6 ' + // PE
    '  AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    'GROUP BY ' +
    '  d.DOC_ID, d.DOC_NRDOCUMENTO, d.DOC_DATAEMISSAO, d.DOC_CHAVENFE, ' +
    '  d.DOC_DATACANCELAMENTONFE, ' +
    '  cdp.CDP_NOME, par.PAR_ID, d.DOC_IDTIPODEOPERACAOSTATUS';
var
  CanceladoCupom, CanceladoMensagem: string;
begin
  LogMensagem('Iniciando migração de pedidos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        CanceladoCupom := QrySQLServer.FieldByName('CANCELADO_CUPOM').AsString;
        CanceladoMensagem := IfThen(CanceladoCupom = 'S', 'CANCELADO', '');

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS (' +
          'CODIGO, TIPO_DOCUMENTO, SIGLA_EMPRESA, FORMA_PAGAMENTO, DATA_EMISSAO, ' +
          'VENDEDOR, CLIENTE, CONTA_RECEBER, VALOR_PRODUTOS, VALOR_DESCONTO, ' +
          'VALOR_TOTAL, IDOPERACAO, CANCELADO_CUPOM, CANCELADO_DATA, CANCELADO_MENSAGEM) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORMA_PAGAMENTO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTA_RECEBER').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_PRODUTOS').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDOPERACAO').AsString) + ', ' +
          QuotedStr(CanceladoCupom) + ', ' +
          IfThen(
            not QrySQLServer.FieldByName('DATA_CANCELAMENTO').IsNull,
            FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CANCELAMENTO').AsDateTime),
            'NULL'
          ) + ', ' +
          QuotedStr(CanceladoMensagem) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir pedido ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de pedidos concluída. Total: ' + IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de pedidos: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarPEProdutos;
const
  SQL_SELECT =
    'WITH RankedItens AS ( ' +
    '    SELECT ' +
    '        ''PE'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '        ''PE'' AS TIPO_DOCUMENTO, ' +
    '        di.DIT_NRITEM AS ITEM_VENDA, ' +
    '        ''001'' AS SIGLA_EMPRESA, ' +
    '        RIGHT(REPLICATE(''0'', 6) + CAST(di.DIT_IDITEM AS VARCHAR(6)), 6) AS PRODUTO, ' +
    '        di.DIT_QTDCOMERCIAL, ' +
    '        di.DIT_VALORUNITARIOCOMERCIAL, ' +
    '        di.DIT_VALORDESCONTO, ' +
    '        di.DIT_IDTIPODEOPERACAOSTATUS, ' +
    '        ROW_NUMBER() OVER ( ' +
    '            PARTITION BY d.DOC_ID, di.DIT_NRITEM  ' +
    '            ORDER BY CASE WHEN di.DIT_IDTIPODEOPERACAOSTATUS <> 34 THEN 1 ELSE 2 END ' +
    '        ) as RowNum ' +
    '    FROM DOCUMENTOS d ' +
    '    JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    '    WHERE d.DOC_IDTIPODEOPERACAO = 6 ' + // PE
    '      AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    ') ' +
    'SELECT  ' +
    '    CODIGO,  ' +
    '    TIPO_DOCUMENTO,  ' +
    '    ITEM_VENDA,  ' +
    '    SIGLA_EMPRESA,  ' +
    '    PRODUTO, ' +
    '    CAST(DIT_QTDCOMERCIAL AS DECIMAL(18,3)) AS QUANTIDADE, ' +
    '    CAST(DIT_VALORUNITARIOCOMERCIAL AS DECIMAL(18,4)) AS VALOR_UNITARIO, ' +
    '    CAST(-ISNULL(DIT_VALORDESCONTO, 0) AS DECIMAL(18,2)) AS VALOR_DESCONTO, ' +
    '    CAST((DIT_QTDCOMERCIAL * DIT_VALORUNITARIOCOMERCIAL) AS DECIMAL(18,2)) AS VALOR_TOTAL, ' +
    '    ''N'' AS REALIZAR_ENTREGA,    ' +
    '    ''N'' AS REALIZAR_MONTAGEM, ' +
    '    ''1'' AS IDICMS ' +
    'FROM RankedItens ' +
    'WHERE RowNum = 1 ' +
    'ORDER BY CODIGO, ITEM_VENDA';
begin
  LogMensagem('Iniciando migração de produtos dos pedidos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS_PRODUTOS (' +
          'CODIGO, TIPO_DOCUMENTO, ITEM_VENDA, SIGLA_EMPRESA, PRODUTO, QUANTIDADE, ' +
          'VALOR_UNITARIO, VALOR_DESCONTO, VALOR_TOTAL, REALIZAR_ENTREGA, REALIZAR_MONTAGEM, IDICMS) ' +
          'VALUES (' +
            QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
            IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('PRODUTO').AsString) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('QUANTIDADE').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_UNITARIO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_ENTREGA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_MONTAGEM').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('IDICMS').AsString) +
          ')';

        QryFirebird.ExecSQL;

      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto do pedido ' +
            QrySQLServer.FieldByName('CODIGO').AsString +
            ', item ' + IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) +
            ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos dos pedidos concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos dos pedidos: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarPEContasReceber;
const
  SQL_SELECT =
  'SELECT ' +
      '  ''PE'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
      '  ROW_NUMBER() OVER (PARTITION BY d.DOC_ID ORDER BY dp.DDU_ID) AS PARCELA, ' +
      '  1 AS SUBPARCELA, ' +
      '  ''001'' AS SIGLA_EMPRESA, ' +
      '  ''N'' AS TIPO_ENTRADA, ' +
      '  RIGHT(REPLICATE(''0'', 9) + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(9)), 9) AS CLIENTE, ' +
      '  dp.DDU_VALORORIGINAL as VALOR, ' +
      '  dp.DDU_VALORORIGINAL as VLR_ORIGINAL, ' +
      '  dp.DDU_DATAEMISSAO as DATA, ' +
      '  dp.DDU_DATAVENCIMENTO as VENCIMENTO, ' +
      '  dpa.DPA_DATAPAGAMENTO as PAGAMENTO, ' +
      '  dp.DDU_VALORJUROS as VLR_JUROS_DIA, ' +
      '  dp.DDU_VALORMULTA as VLR_MULTA, ' +
      '  dp.DDU_VALORDESCONTO as VLR_DESCONTO, ' +
      '  dpa.DPA_VALORPAGO as RECEBIDO, ' +
      '  dp.DDU_VALORTOTALGERAL as VLR_TOTAL ' +
  'FROM DOCUMENTOS d ' +
  'LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
  'LEFT JOIN DOCUMENTOSDUPLICATASPAGAMENTOS dpa ON dpa.DPA_IDDUPLICATAS = dp.DDU_ID ' +
  'WHERE d.DOC_NRDOCUMENTO <> '''' ' +
      '  AND d.DOC_IDTIPODEOPERACAO = 6 ' + // PE
      '  AND dpa.DPA_DATAESTORNO IS NULL ' +  // ignora estornados
      '  AND NOT (dp.DDU_VALORORIGINAL IS NULL AND dp.DDU_DATAEMISSAO IS NULL) ' + // ignora sem valor e sem data
      '  AND dp.DDU_IDTIPODEOPERACAOSTATUS <> 34 ' + // exclui cancelados
  'ORDER BY d.DOC_IDTIPODEOPERACAO, PARCELA';
begin
  LogMensagem('Iniciando migração de Contas a Receber de Pedidos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CONTAS_RECEBER (' +
          'CODIGO, PARCELA, SUBPARCELA, SIGLA_EMPRESA, TIPO_ENTRADA, CLIENTE, ' +
          'VALOR, DATA, VENCIMENTO, PAGAMENTO, VLR_ORIGINAL, VLR_JUROS_DIA, ' +
          'VLR_MULTA, VLR_DESCONTO, RECEBIDO, VLR_TOTAL) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('PARCELA').AsInteger) + ', ' +
          IntToStr(QrySQLServer.FieldByName('SUBPARCELA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_ENTRADA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR').AsFloat) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('VENCIMENTO').AsDateTime) + ', ' +
          IfThen(not QrySQLServer.FieldByName('PAGAMENTO').IsNull,
                 FormatarDataParaSQL(QrySQLServer.FieldByName('PAGAMENTO').AsDateTime),
                 'NULL') + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_ORIGINAL').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_JUROS_DIA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_MULTA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('RECEBIDO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_TOTAL').AsFloat) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir conta a receber do pedido ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de Contas a Receber de Pedidos concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de Contas a Receber de Pedidos: ' + E.Message);
  end;
end;





procedure TMigrador.MigrarCF;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''CF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  ''CF'' AS TIPO_DOCUMENTO, ' +
    '  ''001'' as SIGLA_EMPRESA, ' +
    '  LEFT(cdp.CDP_NOME, 15) AS FORMA_PAGAMENTO, ' +
    '  d.DOC_DATAEMISSAO AS DATA_EMISSAO, ' +
    '  ''000000001'' AS VENDEDOR, ' +
    '  RIGHT(REPLICATE(''0'', 9) + CAST(par.PAR_ID AS VARCHAR(9)), 9) AS CLIENTE, ' +
    '  ''CF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CONTA_RECEBER, ' +
    '  ROUND(SUM(di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL), 2) AS VALOR_PRODUTOS, ' +
    '  ROUND(SUM(ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_DESCONTO, ' +
    '  ROUND(SUM((di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL) - ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_TOTAL, ' +
    '  ''VD'' as IDOPERACAO, ' +
    '  CASE ' +
    '    WHEN d.DOC_IDTIPODEOPERACAOSTATUS IN (31, 32) THEN ''N'' ' +
    '    ELSE ''S'' ' +
    '  END AS CANCELADO_CUPOM, ' +
    '  d.DOC_DATACANCELAMENTONFE AS DATA_CANCELAMENTO ' +
    'FROM DOCUMENTOS d ' +
    'JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    'LEFT JOIN CONDICOESDEPAGAMENTO cdp ON cdp.CDP_ID = d.DOC_IDCONDICOESDEPAGAMENTO ' +
    'LEFT JOIN PARTICIPANTES par ON par.PAR_ID = d.DOC_IDPARTICIPANTE ' +
    'WHERE d.DOC_IDTIPODEOPERACAO = 84 ' + // CF
    '  AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    'GROUP BY ' +
    '  d.DOC_ID, d.DOC_NRDOCUMENTO, d.DOC_DATAEMISSAO, d.DOC_CHAVENFE, ' +
    '  d.DOC_DATACANCELAMENTONFE, ' +
    '  cdp.CDP_NOME, par.PAR_ID, d.DOC_IDTIPODEOPERACAOSTATUS';
var
  CanceladoCupom, CanceladoMensagem: string;
begin
  LogMensagem('Iniciando migração de cupons fiscais...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        CanceladoCupom := QrySQLServer.FieldByName('CANCELADO_CUPOM').AsString;
        CanceladoMensagem := IfThen(CanceladoCupom = 'S', 'CANCELADO', '');

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS (' +
          'CODIGO, TIPO_DOCUMENTO, SIGLA_EMPRESA, FORMA_PAGAMENTO, DATA_EMISSAO, ' +
          'VENDEDOR, CLIENTE, CONTA_RECEBER, VALOR_PRODUTOS, VALOR_DESCONTO, ' +
          'VALOR_TOTAL, IDOPERACAO, CANCELADO_CUPOM, CANCELADO_DATA, CANCELADO_MENSAGEM) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORMA_PAGAMENTO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTA_RECEBER').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_PRODUTOS').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDOPERACAO').AsString) + ', ' +
          QuotedStr(CanceladoCupom) + ', ' +
          IfThen(
            not QrySQLServer.FieldByName('DATA_CANCELAMENTO').IsNull,
            FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CANCELAMENTO').AsDateTime),
            'NULL'
          ) + ', ' +
          QuotedStr(CanceladoMensagem) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir cupom fiscal ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de cupons fiscais concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de cupons fiscais: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarCFProdutos;
const
  SQL_SELECT =
    'WITH RankedItens AS ( ' +
    '    SELECT ' +
    '        ''CF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '        ''CF'' AS TIPO_DOCUMENTO, ' +
    '        di.DIT_NRITEM AS ITEM_VENDA, ' +
    '        ''001'' AS SIGLA_EMPRESA, ' +
    '        RIGHT(REPLICATE(''0'', 6) + CAST(di.DIT_IDITEM AS VARCHAR(6)), 6) AS PRODUTO, ' +
    '        di.DIT_QTDCOMERCIAL, ' +
    '        di.DIT_VALORUNITARIOCOMERCIAL, ' +
    '        di.DIT_VALORDESCONTO, ' +
    '        di.DIT_IDTIPODEOPERACAOSTATUS, ' +
    '        ROW_NUMBER() OVER ( ' +
    '            PARTITION BY d.DOC_ID, di.DIT_NRITEM ' +
    '            ORDER BY CASE WHEN di.DIT_IDTIPODEOPERACAOSTATUS <> 34 THEN 1 ELSE 2 END ' +
    '        ) as RowNum ' +
    '    FROM DOCUMENTOS d ' +
    '    JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    '    WHERE d.DOC_IDTIPODEOPERACAO = 84 ' + // CF
    '      AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    ') ' +
    'SELECT  ' +
    '    CODIGO,  ' +
    '    TIPO_DOCUMENTO,  ' +
    '    ITEM_VENDA,  ' +
    '    SIGLA_EMPRESA,  ' +
    '    PRODUTO, ' +
    '    CAST(DIT_QTDCOMERCIAL AS DECIMAL(18,3)) AS QUANTIDADE, ' +
    '    CAST(DIT_VALORUNITARIOCOMERCIAL AS DECIMAL(18,4)) AS VALOR_UNITARIO, ' +
    '    CAST(-ISNULL(DIT_VALORDESCONTO, 0) AS DECIMAL(18,2)) AS VALOR_DESCONTO, ' +
    '    CAST((DIT_QTDCOMERCIAL * DIT_VALORUNITARIOCOMERCIAL) AS DECIMAL(18,2)) AS VALOR_TOTAL, ' +
    '    ''N'' AS REALIZAR_ENTREGA,    ' +
    '    ''N'' AS REALIZAR_MONTAGEM, ' +
    '    ''1'' AS IDICMS ' +
    'FROM RankedItens ' +
    'WHERE RowNum = 1 ' +
    'ORDER BY CODIGO, ITEM_VENDA';
begin
  LogMensagem('Iniciando migração de produtos dos cupons fiscais...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS_PRODUTOS (' +
          'CODIGO, TIPO_DOCUMENTO, ITEM_VENDA, SIGLA_EMPRESA, PRODUTO, QUANTIDADE, ' +
          'VALOR_UNITARIO, VALOR_DESCONTO, VALOR_TOTAL, REALIZAR_ENTREGA, REALIZAR_MONTAGEM, IDICMS) ' +
          'VALUES (' +
            QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
            IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('PRODUTO').AsString) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('QUANTIDADE').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_UNITARIO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_ENTREGA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_MONTAGEM').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('IDICMS').AsString) +
          ')';

        QryFirebird.ExecSQL;

      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto do cupom ' +
            QrySQLServer.FieldByName('CODIGO').AsString +
            ', item ' + IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) +
            ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos dos cupons concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos dos cupons: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarCFContasReceber;
const
  SQL_SELECT =
  'SELECT ' +
      '  ''CF'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
      '  ROW_NUMBER() OVER (PARTITION BY d.DOC_ID ORDER BY dp.DDU_ID) AS PARCELA, ' +
      '  1 AS SUBPARCELA, ' +
      '  ''001'' AS SIGLA_EMPRESA, ' +
      '  ''N'' AS TIPO_ENTRADA, ' +
      '  RIGHT(REPLICATE(''0'', 9) + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(9)), 9) AS CLIENTE, ' +
      '  dp.DDU_VALORORIGINAL as VALOR, ' +
      '  dp.DDU_VALORORIGINAL as VLR_ORIGINAL, ' +
      '  dp.DDU_DATAEMISSAO as DATA, ' +
      '  dp.DDU_DATAVENCIMENTO as VENCIMENTO, ' +
      '  dpa.DPA_DATAPAGAMENTO as PAGAMENTO, ' +
      '  dp.DDU_VALORJUROS as VLR_JUROS_DIA, ' +
      '  dp.DDU_VALORMULTA as VLR_MULTA, ' +
      '  dp.DDU_VALORDESCONTO as VLR_DESCONTO, ' +
      '  dpa.DPA_VALORPAGO as RECEBIDO, ' +
      '  dp.DDU_VALORTOTALGERAL as VLR_TOTAL ' +
  'FROM DOCUMENTOS d ' +
  'LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
  'LEFT JOIN DOCUMENTOSDUPLICATASPAGAMENTOS dpa ON dpa.DPA_IDDUPLICATAS = dp.DDU_ID ' +
  'WHERE d.DOC_NRDOCUMENTO <> '''' ' +
      '  AND d.DOC_IDTIPODEOPERACAO = 84 ' + // CF
      '  AND dpa.DPA_DATAESTORNO IS NULL ' +  // ignora estornados
      '  AND NOT (dp.DDU_VALORORIGINAL IS NULL AND dp.DDU_DATAEMISSAO IS NULL) ' + // ignora sem valor e sem data
      '  AND dp.DDU_IDTIPODEOPERACAOSTATUS <> 34 ' + // exclui cancelados
  'ORDER BY d.DOC_IDTIPODEOPERACAO, PARCELA';
begin
  LogMensagem('Iniciando migração de Contas a Receber de Cupons Fiscais...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CONTAS_RECEBER (' +
          'CODIGO, PARCELA, SUBPARCELA, SIGLA_EMPRESA, TIPO_ENTRADA, CLIENTE, ' +
          'VALOR, DATA, VENCIMENTO, PAGAMENTO, VLR_ORIGINAL, VLR_JUROS_DIA, ' +
          'VLR_MULTA, VLR_DESCONTO, RECEBIDO, VLR_TOTAL) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('PARCELA').AsInteger) + ', ' +
          IntToStr(QrySQLServer.FieldByName('SUBPARCELA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_ENTRADA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR').AsFloat) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('VENCIMENTO').AsDateTime) + ', ' +
          IfThen(not QrySQLServer.FieldByName('PAGAMENTO').IsNull,
                 FormatarDataParaSQL(QrySQLServer.FieldByName('PAGAMENTO').AsDateTime),
                 'NULL') + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_ORIGINAL').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_JUROS_DIA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_MULTA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('RECEBIDO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_TOTAL').AsFloat) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir conta a receber do cupom ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de Contas a Receber de Cupons concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de Contas a Receber de Cupons: ' + E.Message);
  end;
end;





procedure TMigrador.MigrarVendasXSAT;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''C1'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  CAST(d.DOC_NRDOCUMENTO AS VARCHAR(9)) AS EXTRATO, ' +
    '  d.DOC_CHAVENFE AS XML, ' +
    '  NULL AS ARQUIVO, ' +
    '  d.DOC_DATAENVIONFE AS DATA_ENVIO, ' +
    '  d.DOC_DATACANCELAMENTONFE AS DATA_CANCELAMENTO, ' +
    '  CASE WHEN d.DOC_DATACANCELAMENTONFE IS NOT NULL THEN d.DOC_CHAVENFE END AS XML_CANCELAMENTO ' +
    'FROM DOCUMENTOS d ' +
    'WHERE d.DOC_IDTIPODEOPERACAO = 261';
begin
  LogMensagem('Iniciando migração de VENDASXSAT...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        var Codigo := QrySQLServer.FieldByName('CODIGO').AsString;
        var Extrato := QrySQLServer.FieldByName('EXTRATO').AsString;
        var Xml := QrySQLServer.FieldByName('XML').AsString;
        var Arquivo := QrySQLServer.FieldByName('ARQUIVO').AsString;
        var XmlCancelamento := QrySQLServer.FieldByName('XML_CANCELAMENTO').AsString;

        var DataSQL: string;
        DataSQL := 'NULL';

        if not QrySQLServer.FieldByName('DATA_ENVIO').IsNull then
          DataSQL := FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_ENVIO').AsDateTime);

        if not QrySQLServer.FieldByName('DATA_CANCELAMENTO').IsNull then
          DataSQL := FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CANCELAMENTO').AsDateTime);

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDASXSAT (CODIGO, EXTRATO, XML, ARQUIVO, DATA, XML_CANCELAMENTO) VALUES (' +
          QuotedStr(Codigo) + ', ' +
          QuotedStr(Extrato) + ', ' +
          QuotedStr(Xml) + ', ' +
          IfThen(Arquivo = '', 'NULL', QuotedStr(Arquivo)) + ', ' +
          DataSQL + ', ' +
          IfThen(XmlCancelamento = '', 'NULL', QuotedStr(XmlCancelamento)) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir VENDASXSAT ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de VENDASXSAT concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de VENDASXSAT: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarSAT;
const
  SQL_SELECT =
    'SELECT ' +
    '  ''C1'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '  ''C1'' AS TIPO_DOCUMENTO, ' +
    '  ''001'' as SIGLA_EMPRESA, ' +
    '  LEFT(cdp.CDP_NOME, 15) AS FORMA_PAGAMENTO, ' +
    '  d.DOC_DATAEMISSAO AS DATA_EMISSAO, ' +
    '  ''000000001'' AS VENDEDOR, ' +
    '  RIGHT(REPLICATE(''0'', 9) + CAST(par.PAR_ID AS VARCHAR(9)), 9) AS CLIENTE, ' +
    '  ''C1'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CONTA_RECEBER, ' +
    '  ROUND(SUM(di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL), 2) AS VALOR_PRODUTOS, ' +
    '  ROUND(SUM(ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_DESCONTO, ' +
    '  ROUND(SUM((di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL) - ISNULL(di.DIT_VALORDESCONTO,0)), 2) AS VALOR_TOTAL, ' +
    '  ''VD'' as IDOPERACAO, ' +
    '  CASE ' +
    '    WHEN d.DOC_IDTIPODEOPERACAOSTATUS IN (31, 32) THEN ''N'' ' +
    '    ELSE ''S'' ' +
    '  END AS CANCELADO_CUPOM, ' +
    '  d.DOC_DATACANCELAMENTONFE AS DATA_CANCELAMENTO ' +
    'FROM DOCUMENTOS d ' +
    'JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    'LEFT JOIN CONDICOESDEPAGAMENTO cdp ON cdp.CDP_ID = d.DOC_IDCONDICOESDEPAGAMENTO ' +
    'LEFT JOIN PARTICIPANTES par ON par.PAR_ID = d.DOC_IDPARTICIPANTE ' +
    'WHERE d.DOC_IDTIPODEOPERACAO = 261 ' +  //C1
    '  AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    'GROUP BY ' +
    '  d.DOC_ID, d.DOC_NRDOCUMENTO, d.DOC_DATAEMISSAO, d.DOC_CHAVENFE, ' +
    '  d.DOC_DATACANCELAMENTONFE, ' +
    '  cdp.CDP_NOME, par.PAR_ID, d.DOC_IDTIPODEOPERACAOSTATUS';
begin
  LogMensagem('Iniciando migração de documentos SAT...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        var CanceladoCupom := QrySQLServer.FieldByName('CANCELADO_CUPOM').AsString;
        var CanceladoMensagem := IfThen(CanceladoCupom = 'S', 'CANCELADO', '');

        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS (' +
          'CODIGO, TIPO_DOCUMENTO, SIGLA_EMPRESA, FORMA_PAGAMENTO, DATA_EMISSAO, ' +
          'VENDEDOR, CLIENTE, CONTA_RECEBER, VALOR_PRODUTOS, VALOR_DESCONTO, ' +
          'VALOR_TOTAL, IDOPERACAO, CANCELADO_CUPOM, CANCELADO_DATA, CANCELADO_MENSAGEM) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORMA_PAGAMENTO').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('VENDEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CONTA_RECEBER').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_PRODUTOS').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('IDOPERACAO').AsString) + ', ' +
          QuotedStr(CanceladoCupom) + ', ' +
          IfThen(
            not QrySQLServer.FieldByName('DATA_CANCELAMENTO').IsNull,
            FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_CANCELAMENTO').AsDateTime),
            'NULL'
          ) + ', ' +
          QuotedStr(CanceladoMensagem) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir documento SAT ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de documentos SAT concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de documentos SAT: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarSATProdutos;
const
  SQL_SELECT =
    'WITH RankedItens AS ( ' +
    '    SELECT ' +
    '        ''C1'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
    '        ''C1'' AS TIPO_DOCUMENTO, ' +
    '        di.DIT_NRITEM AS ITEM_VENDA, ' +
    '        ''001'' AS SIGLA_EMPRESA, ' +
    '        RIGHT(REPLICATE(''0'', 6) + CAST(di.DIT_IDITEM AS VARCHAR(6)), 6) AS PRODUTO, ' +
    '        di.DIT_QTDCOMERCIAL, ' +
    '        di.DIT_VALORUNITARIOCOMERCIAL, ' +
    '        di.DIT_VALORDESCONTO, ' +
    '        ROW_NUMBER() OVER ( ' +
    '            PARTITION BY d.DOC_ID, di.DIT_NRITEM ' +
    '            ORDER BY di.DIT_NRITEM ' +
    '        ) as RowNum ' +
    '    FROM DOCUMENTOS d ' +
    '    JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    '    WHERE d.DOC_IDTIPODEOPERACAO = 261 ' +
    '      AND ISNULL(di.DIT_IDTIPODEOPERACAOSTATUS, 0) <> 34 ' +
    ') ' +
    'SELECT  ' +
    '    CODIGO,  ' +
    '    TIPO_DOCUMENTO,  ' +
    '    ITEM_VENDA,  ' +
    '    SIGLA_EMPRESA,  ' +
    '    PRODUTO, ' +
    '    CAST(DIT_QTDCOMERCIAL AS DECIMAL(18,3)) AS QUANTIDADE, ' +
    '    CAST(DIT_VALORUNITARIOCOMERCIAL AS DECIMAL(18,4)) AS VALOR_UNITARIO, ' +
    '    CAST(-ISNULL(DIT_VALORDESCONTO, 0) AS DECIMAL(18,2)) AS VALOR_DESCONTO, ' +
    '    CAST((DIT_QTDCOMERCIAL * DIT_VALORUNITARIOCOMERCIAL) AS DECIMAL(18,2)) AS VALOR_TOTAL, ' +
    '    ''N'' AS REALIZAR_ENTREGA,    ' +
    '    ''N'' AS REALIZAR_MONTAGEM, ' +
    '    ''1'' AS IDICMS ' +
    'FROM RankedItens ' +
    'WHERE RowNum = 1 ' +
    'ORDER BY CODIGO, ITEM_VENDA';
begin
  LogMensagem('Iniciando migração de produtos dos documentos SAT...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO VENDAS_PRODUTOS (' +
          'CODIGO, TIPO_DOCUMENTO, ITEM_VENDA, SIGLA_EMPRESA, PRODUTO, QUANTIDADE, ' +
          'VALOR_UNITARIO, VALOR_DESCONTO, VALOR_TOTAL, REALIZAR_ENTREGA, REALIZAR_MONTAGEM, IDICMS) ' +
          'VALUES (' +
            QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
            IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('PRODUTO').AsString) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('QUANTIDADE').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_UNITARIO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
            FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_ENTREGA').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('REALIZAR_MONTAGEM').AsString) + ', ' +
            QuotedStr(QrySQLServer.FieldByName('IDICMS').AsString) +
          ')';

        QryFirebird.ExecSQL;

      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto do documento SAT ' +
            QrySQLServer.FieldByName('CODIGO').AsString +
            ', item ' + IntToStr(QrySQLServer.FieldByName('ITEM_VENDA').AsInteger) +
            ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de produtos dos documentos SAT concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos dos documentos SAT: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarSATContasReceber;
const
  SQL_SELECT =
  'SELECT ' +
      '  ''C1'' + RIGHT(''0000000'' + CAST(d.DOC_NRDOCUMENTO AS VARCHAR(7)), 7) AS CODIGO, ' +
      '  ROW_NUMBER() OVER (PARTITION BY d.DOC_ID ORDER BY dp.DDU_ID) AS PARCELA, ' +
      '  1 AS SUBPARCELA, ' +
      '  ''001'' AS SIGLA_EMPRESA, ' +
      '  ''N'' AS TIPO_ENTRADA, ' +
      '  RIGHT(REPLICATE(''0'', 9) + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(9)), 9) AS CLIENTE, ' +
      '  dp.DDU_VALORORIGINAL as VALOR, ' +
      '  dp.DDU_VALORORIGINAL as VLR_ORIGINAL, ' +
      '  dp.DDU_DATAEMISSAO as DATA, ' +
      '  dp.DDU_DATAVENCIMENTO as VENCIMENTO, ' +
      '  dpa.DPA_DATAPAGAMENTO as PAGAMENTO, ' +
      '  dp.DDU_VALORJUROS as VLR_JUROS_DIA, ' +
      '  dp.DDU_VALORMULTA as VLR_MULTA, ' +
      '  dp.DDU_VALORDESCONTO as VLR_DESCONTO, ' +
      '  dpa.DPA_VALORPAGO as RECEBIDO, ' +
      '  dp.DDU_VALORTOTALGERAL as VLR_TOTAL ' +
  'FROM DOCUMENTOS d ' +
  'LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
  'LEFT JOIN DOCUMENTOSDUPLICATASPAGAMENTOS dpa ON dpa.DPA_IDDUPLICATAS = dp.DDU_ID ' +
  'WHERE d.DOC_NRDOCUMENTO <> '''' ' +
      '  AND d.DOC_IDTIPODEOPERACAO = 261 ' + // C1
      '  AND dpa.DPA_DATAESTORNO IS NULL ' +  // ignora estornados
      '  AND NOT (dp.DDU_VALORORIGINAL IS NULL AND dp.DDU_DATAEMISSAO IS NULL) ' + // ignora sem valor e sem data
      '  AND dp.DDU_IDTIPODEOPERACAOSTATUS <> 34 ' + // exclui cancelados
  'ORDER BY d.DOC_IDTIPODEOPERACAO, PARCELA';
begin
  LogMensagem('Iniciando migração de Contas a Receber de documentos SAT...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CONTAS_RECEBER (' +
          'CODIGO, PARCELA, SUBPARCELA, SIGLA_EMPRESA, TIPO_ENTRADA, CLIENTE, ' +
          'VALOR, DATA, VENCIMENTO, PAGAMENTO, VLR_ORIGINAL, VLR_JUROS_DIA, ' +
          'VLR_MULTA, VLR_DESCONTO, RECEBIDO, VLR_TOTAL) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('PARCELA').AsInteger) + ', ' +
          IntToStr(QrySQLServer.FieldByName('SUBPARCELA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_ENTRADA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CLIENTE').AsString) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR').AsFloat) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('VENCIMENTO').AsDateTime) + ', ' +
          IfThen(not QrySQLServer.FieldByName('PAGAMENTO').IsNull,
                 FormatarDataParaSQL(QrySQLServer.FieldByName('PAGAMENTO').AsDateTime),
                 'NULL') + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_ORIGINAL').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_JUROS_DIA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_MULTA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('RECEBIDO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_TOTAL').AsFloat) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir conta a receber do documento SAT ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de Contas a Receber de documentos SAT concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de Contas a Receber de documentos SAT: ' + E.Message);
  end;
end;





procedure TMigrador.MigrarNFCompra;
const
  SQL_SELECT =
    'WITH NF_AGRUPADA AS (' +
    '  SELECT ' +
    '    d.DOC_ID, ' +
    '    d.DOC_IDPARTICIPANTE, ' +
    '    d.DOC_NRDOCUMENTO, ' +
    '    d.DOC_CHAVENFE, ' +
    '    MIN(dp.DDU_DATAEMISSAO) AS DATA_EMISSAO, ' +
    '    SUM(dp.DDU_VALORTOTALGERAL) AS VALOR_TOTAL ' +
    '  FROM DOCUMENTOS d ' +
    '  LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
    '  WHERE d.DOC_IDTIPODEOPERACAO = 5 ' +  // NC
    '    AND d.DOC_IDTIPODEOPERACAOSTATUS <> 34 ' +
    '    AND d.DOC_NRDOCUMENTO IS NOT NULL ' +
    '  GROUP BY d.DOC_ID, d.DOC_IDPARTICIPANTE, d.DOC_NRDOCUMENTO, d.DOC_CHAVENFE' +
    ')' +
    'SELECT ' +
    '  RIGHT(''000000000'' + CAST(DOC_ID AS VARCHAR(9)), 9) AS CODIGO, ' +
    '  ''001'' AS SIGLA_EMPRESA, ' +
    '  ''NC'' AS TIPO_DOCUMENTO, ' +
    '  ''C'' AS TIPO_MOVIMENTO, ' +
    '  ''000007'' AS OPERACAO_FISCAL, ' +
    '  RIGHT(''000000'' + CAST(DOC_IDPARTICIPANTE AS VARCHAR(6)),6) AS FORNECEDOR, ' +
    '  DOC_NRDOCUMENTO AS NUMERO_NF_FORNECEDOR, ' +
    '  DATA_EMISSAO, ' +
    '  DATA_EMISSAO AS DATA_INC, ' +
    '  VALOR_TOTAL, ' +
    '  DOC_CHAVENFE AS CHAVE_NFE ' +
    'FROM NF_AGRUPADA ' +
    'ORDER BY DOC_ID';
begin
  LogMensagem('Iniciando migração de NF de Compra');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO NF_COMPRA (' +
          'CODIGO, SIGLA_EMPRESA, TIPO_DOCUMENTO, TIPO_MOVIMENTO, OPERACAO_FISCAL, ' +
          'FORNECEDOR, NUMERO_NF_FORNECEDOR, DATA_EMISSAO, DATA_INC, VALOR_TOTAL, CHAVE_NFE) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_MOVIMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('OPERACAO_FISCAL').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORNECEDOR').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NUMERO_NF_FORNECEDOR').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_INC').AsDateTime) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('CHAVE_NFE').AsString) +
          ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir NF Compra ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;

      QrySQLServer.Next;
    end;

    LogMensagem('Migração de NF de Compra concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de NF de Compra: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarNFContasPagar;
const
  SQL_SELECT =
  'SELECT ' +
      '  RIGHT(''000000000'' + CAST(d.DOC_ID AS VARCHAR(9)), 9) AS CODIGO, ' +
      '  ROW_NUMBER() OVER (PARTITION BY d.DOC_ID ORDER BY dp.DDU_ID) AS PARCELA, ' +
      '  1 AS SUBPARCELA, ' +
      '  ''001'' AS SIGLA_EMPRESA, ' +
      '  ''NC'' AS TIPO_DOCUMENTO, ' +
      '  ''DUPL'' AS TIPO_TITULO, ' +
      '  RIGHT(''000000'' + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(6)),6) AS FORNECEDOR, ' +
      '  dp.DDU_DATAEMISSAO AS DATA_EMISSAO, ' +
      '  dp.DDU_DATAVENCIMENTO AS DATA_VCTO, ' +
      '  dpa.DPA_DATAPAGAMENTO AS DATA_PGTO, ' +
      '  dp.DDU_VALORORIGINAL AS VLR_ORIGINAL, ' +
      '  dp.DDU_VALORJUROS AS VLR_JUROS_DIA, ' +
      '  dp.DDU_VALORMULTA AS VLR_MULTA, ' +
      '  dp.DDU_VALORDESCONTO AS VLR_DESCONTO, ' +
      '  dp.DDU_VALORTOTALGERAL AS VLR_TOTAL, ' +
      '  d.DOC_NRDOCUMENTO AS NRO_DOC_FORNECEDOR ' +
  'FROM DOCUMENTOS d ' +
  'LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
  'LEFT JOIN DOCUMENTOSDUPLICATASPAGAMENTOS dpa ON dpa.DPA_IDDUPLICATAS = dp.DDU_ID ' +
  'WHERE d.DOC_IDTIPODEOPERACAO = 5 ' +  // NC
      '  AND NOT (dp.DDU_VALORORIGINAL IS NULL AND dp.DDU_DATAEMISSAO IS NULL) ' +
      '  AND dp.DDU_IDTIPODEOPERACAOSTATUS <> 34 ' + // exclui cancelados
  'ORDER BY d.DOC_ID, dp.DDU_ID';
begin
  LogMensagem('Iniciando migração de Contas a Pagar...');
  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Registros encontrados: ' +
      IntToStr(QrySQLServer.RecordCount));
    while not QrySQLServer.Eof do
    begin
      try
        QryFirebird.SQL.Text :=
          'INSERT INTO CONTAS_PAGAR (' +
          'CODIGO, PARCELA, SUBPARCELA, SIGLA_EMPRESA, TIPO_DOCUMENTO, TIPO_TITULO, FORNECEDOR, ' +
          'DATA_EMISSAO, DATA_VCTO, DATA_PGTO, PORTADOR, CONTA_CORRENTE, VLR_ORIGINAL, VLR_JUROS_DIA, ' +
          'VLR_MULTA, VLR_DESCONTO, VLR_TOTAL, NRO_DOC_FORNECEDOR, STATUS_CONFERIDO, STATUS_LIBERACAO) VALUES (' +
          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
          IntToStr(QrySQLServer.FieldByName('PARCELA').AsInteger) + ', ' +
          IntToStr(QrySQLServer.FieldByName('SUBPARCELA').AsInteger) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_DOCUMENTO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('TIPO_TITULO').AsString) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('FORNECEDOR').AsString) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_VCTO').AsDateTime) + ', ' +
          IfThen(not QrySQLServer.FieldByName('DATA_PGTO').IsNull,
                 FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_PGTO').AsDateTime),
                 'NULL') + ', ' +
          '''CARTEIRA'', ' +
          '''DINHEIRO.001'', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_ORIGINAL').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_JUROS_DIA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_MULTA').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_DESCONTO').AsFloat) + ', ' +
          FormatFloatParaSQL(QrySQLServer.FieldByName('VLR_TOTAL').AsFloat) + ', ' +
          QuotedStr(QrySQLServer.FieldByName('NRO_DOC_FORNECEDOR').AsString) + ', ' +
          '''S'', ''S''' +
          ')';
        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir Contas a Pagar ' +
            QrySQLServer.FieldByName('CODIGO').AsString + ': ' + E.Message);
      end;
      QrySQLServer.Next;
    end;
    LogMensagem('Migração de Contas a Pagar concluída. Total: ' +
      IntToStr(QrySQLServer.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de Contas a Pagar: ' + E.Message);
  end;
end;

procedure TMigrador.MigrarNFCompraProdutos;
const
  SQL_SELECT =
    'SELECT ' +
    '  RIGHT(''000000000'' + CAST(di.DIT_IDDOCUMENTOS AS VARCHAR(9)), 9) AS CODIGO, ' +
    '  RIGHT(''000000'' + CAST(d.DOC_IDPARTICIPANTE AS VARCHAR(6)),6) AS CODIGO_FORNECEDOR, ' +
    '  ''001'' AS SIGLA_EMPRESA, ' +
    '  RIGHT(''000000'' + CAST(di.DIT_IDITEM AS VARCHAR(6)),6) AS PRODUTO, ' +
    '  ''201'' AS CLAS_DESPESA, ' +
    '  di.DIT_QTDCOMERCIAL AS QUANTIDADE, ' +
    '  di.DIT_VALORUNITARIOCOMERCIAL AS VALOR_UNITARIO, ' +
    '  (di.DIT_QTDCOMERCIAL * di.DIT_VALORUNITARIOCOMERCIAL) AS VALOR_TOTAL, ' +
    '  ISNULL(di.DIT_VALORDESCONTO, 0) AS VALOR_DESCONTO, ' +
    '  MIN(dp.DDU_DATAEMISSAO) AS DATA_EMISSAO ' +
    'FROM DOCUMENTOS d ' +
    '  LEFT JOIN DOCUMENTOSITENS di ON di.DIT_IDDOCUMENTOS = d.DOC_ID ' +
    '  LEFT JOIN DOCUMENTOSDUPLICATAS dp ON dp.DDU_IDDOCUMENTOS = d.DOC_ID ' +
    'WHERE d.DOC_IDTIPODEOPERACAO = 5 ' +  // NF Compra
    '  AND d.DOC_IDTIPODEOPERACAOSTATUS <> 34 ' +
    '  AND d.DOC_NRDOCUMENTO IS NOT NULL ' +
    'GROUP BY ' +
    '  di.DIT_IDDOCUMENTOS, d.DOC_IDPARTICIPANTE, di.DIT_IDITEM, ' +
    '  di.DIT_QTDCOMERCIAL, di.DIT_VALORUNITARIOCOMERCIAL, di.DIT_VALORDESCONTO ' +
    'ORDER BY di.DIT_IDDOCUMENTOS';
begin
  // A COMPRA VEM COM PRODUTO REPETIDO O QUE MUDA EH O CODBARRAS

//  LogMensagem('Iniciando migração de NF Compra Produtos -> ORDENS_COMPRA_PRODUTOS');
//
//  try
//    ExecutarConsultaOrigem(SQL_SELECT);
//    LogMensagem('Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));
//
//    while not QrySQLServer.Eof do
//    begin
//      try
//        QryFirebird.SQL.Text :=
//          'INSERT INTO ORDENS_COMPRA_PRODUTOS (' +
//          'CODIGO, PRODUTO, SIGLA_EMPRESA, CODIGO_FORNECEDOR, ' +
//          'CLAS_DESPESA, QUANTIDADE, VALOR_UNITARIO, VALOR_TOTAL, ' +
//          'VALOR_DESCONTO, DATA_INC, USU_INC) VALUES (' +
//          QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
//          QuotedStr(QrySQLServer.FieldByName('PRODUTO').AsString) + ', ' +
//          QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
//          QuotedStr(QrySQLServer.FieldByName('CODIGO_FORNECEDOR').AsString) + ', ' +
//          QuotedStr(QrySQLServer.FieldByName('CLAS_DESPESA').AsString) + ', ' +
//          FormatFloatParaSQL(QrySQLServer.FieldByName('QUANTIDADE').AsFloat) + ', ' +
//          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_UNITARIO').AsFloat) + ', ' +
//          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_TOTAL').AsFloat) + ', ' +
//          FormatFloatParaSQL(QrySQLServer.FieldByName('VALOR_DESCONTO').AsFloat) + ', ' +
//          FormatarDataParaSQL(QrySQLServer.FieldByName('DATA_EMISSAO').AsDateTime) + ', ' +
//          QuotedStr('MIGRADOR') + ')';
//
//        QryFirebird.ExecSQL;
//      except
//        on E: Exception do
//          LogMensagem('Erro ao inserir produto da NF ' +
//            QrySQLServer.FieldByName('CODIGO').AsString + ' - ' +
//            QrySQLServer.FieldByName('PRODUTO').AsString + ': ' + E.Message);
//      end;
//
//      QrySQLServer.Next;
//    end;
//
//    LogMensagem('Migração de produtos da NF Compra concluída. Total: ' +
//      IntToStr(QrySQLServer.RecordCount));
//  except
//    on E: Exception do
//      LogMensagem('Erro durante migração de NF Compra Produtos: ' + E.Message);
//  end;
end;












end.
