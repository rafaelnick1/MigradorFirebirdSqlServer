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
  FireDAC.Stan.Def, FireDAC.Stan.Pool, System.IniFiles, System.IOUtils;

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
    function Conexao: Boolean;
    procedure MigrarClientes;
    procedure MigrarFornecedores;
    procedure MigrarGrupos;
    procedure MigrarProdutosPorFornecedor;
    procedure MigrarProdutos;
    procedure MigrarProdutosPorEmpresa;
  public
    { Public declarations }
  end;

var
  Migrador: TMigrador;

implementation

{$R *.dfm}

function TMigrador.Conexao: Boolean;
var
  Ini: TIniFile;
  IniPath: string;
begin
  Result := False;

  IniPath := TPath.Combine(ExtractFilePath(ParamStr(0)), 'Config.ini');
  if not FileExists(IniPath) then
  begin
    MemoLog.Lines.Add('Arquivo Config.ini n�o encontrado!');
    Exit;
  end;

  Ini := TIniFile.Create(IniPath);
  try
    // Conex�o Firebird
    try
      FDConnFirebird.Params.Clear;
      FDConnFirebird.DriverName := 'FB';
      FDConnFirebird.Params.Add('Database=' + Ini.ReadString('Firebird', 'Database', ''));
      FDConnFirebird.Params.Add('User_Name=' + Ini.ReadString('Firebird', 'User_Name', ''));
      FDConnFirebird.Params.Add('Password=' + Ini.ReadString('Firebird', 'Password', ''));
      FDConnFirebird.Params.Add('CharacterSet=' + Ini.ReadString('Firebird', 'CharacterSet', 'WIN1252'));
      FDConnFirebird.Connected := True;
      MemoLog.Lines.Add('Conectado ao Firebird com sucesso!');
    except
      on E: Exception do
      begin
        MemoLog.Lines.Add('Erro ao conectar Firebird: ' + E.Message);
        Exit;
      end;
    end;

    // Conex�o SQL Server
    try
      FDConnSQLServer.Params.Clear;
      FDConnSQLServer.DriverName := 'MSSQL';
      FDConnSQLServer.Params.Add('Server=' + Ini.ReadString('SQLServer', 'Server', ''));
      FDConnSQLServer.Params.Add('Database=' + Ini.ReadString('SQLServer', 'Database', ''));
      FDConnSQLServer.Params.Add('OSAuthent=' + Ini.ReadString('SQLServer', 'OSAuthent', 'Yes'));
      FDConnSQLServer.Connected := True;
      MemoLog.Lines.Add('Conectado ao SQL Server com sucesso!');
    except
      on E: Exception do
      begin
        MemoLog.Lines.Add('Erro ao conectar SQL Server: ' + E.Message);
        Exit;
      end;
    end;

    Result := True;
  finally
    Ini.Free;
  end;
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

procedure TMigrador.btnMigrarClick(Sender: TObject);
begin
  MemoLog.Lines.Clear;

  if not Conexao then
    Exit;

  MigrarClientes;
  MigrarFornecedores;
  MigrarGrupos;
  MigrarProdutosPorFornecedor;
  MigrarProdutos;
  MigrarProdutosPorEmpresa;
end;

procedure TMigrador.MigrarClientes;
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
    'CASE WHEN PAR.PAR_TIPOPESSOA = ''F'' THEN ''F�sica'' ELSE ''Jur�dica'' END AS PESSOA, ' +
    'PAR.PAR_CPFCNPJ AS CPF_CNPJ, ' +
    'PAR.PAR_RGINSCRICAOESTADUAL AS RG_INSCRICAO, ' +
    'LEFT(PAR.PAR_OBSERVACAO, 2000) AS OBSERVACOES, ' +
    'PAR.PAR_DATADECADASTRO AS DATA_INC, ' +
    'PAR.PAR_DATAMODIFICACAO AS DATA_ALT, ' +
    'PAR.PAR_DATADECADASTRO AS DATA_CADASTRO, ' +
    '''N'' AS VENDA_CONVENIO, ' +
    '''S'' AS EMITE_CARTA_COBRANCA, ' +
    '''S'' AS EMITE_ALERTA, ' +
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
    'CASE WHEN PAR.PAR_IDTIPODEOPERACAOSTATUS = 140 THEN ''S'' ELSE ''N'' END AS STATUS ' +
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
    'AND PTO.PTO_IDTIPODEOPERACAO = 12';

var
  InsertSQL: string;
  Contador: Integer;
begin
  try
    QryFirebird.SQL.Text := 'DELETE FROM VENDAS_PRODUTOS';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela VENDAS_PRODUTOS limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar VENDAS_PRODUTOS: ' + E.Message);
  end;

  try
    QryFirebird.SQL.Text := 'DELETE FROM VENDASXCFOP';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela VENDASXCFOP limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar VENDASXCFOP: ' + E.Message);
  end;

  try
    QryFirebird.SQL.Text := 'DELETE FROM VENDAS';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela VENDAS limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar VENDAS: ' + E.Message);
  end;

  try
    QryFirebird.SQL.Text := 'DELETE FROM CLIENTES';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela CLIENTES limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar CLIENTES: ' + E.Message);
  end;

  MemoLog.Lines.Add('Iniciando etapa: MigrarClientes...');

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta SQL Server executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);

    MemoLog.Lines.Add('Inserindo cliente ' + IntToStr(Contador) + ' - CODIGO: ' + QrySQLServer.FieldByName('CODIGO').AsString);

    InsertSQL :=
      'INSERT INTO CLIENTES (' +
      'CODIGO, NOME, FANTASIA, ENDERECO, COMPLEMENTO, BAIRRO, CIDADE, CEP, ESTADO, TELEFONE01, ' +
      'TELEFONE02, CONTATO, DATA_NASCIMENTO, PESSOA, CPF_CNPJ, RG_INSCRICAO, OBSERVACOES, DATA_INC, DATA_ALT, DATA_CADASTRO, ' +
      'VENDA_CONVENIO, EMITE_CARTA_COBRANCA, EMITE_ALERTA, CASA_PROPRIA, NOME_PAI, NOME_MAE, NOME_CONJUGE, CPF_CONJUGE, RG_CONJUGE, DATA_NASCIMENTO_CONJUGE, ' +
      'LOCAL_TRABALHO_CONJUGE, LIMITE_CREDITO, VENDE_VAREJO, VENDE_ATACADO, COBRAR_JUROS, CONSUMIDOR_FINAL, NUMERO, IDCIDADE, CONTRIBUINTE, STATUS) ' +
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
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_NASCIMENTO').AsDateTime)) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('PESSOA').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CPF_CNPJ').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('RG_INSCRICAO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('OBSERVACOES').AsString) + ', ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_INC').AsDateTime)) + ', ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_ALT').AsDateTime)) + ', ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_CADASTRO').AsDateTime)) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('VENDA_CONVENIO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('EMITE_CARTA_COBRANCA').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('EMITE_ALERTA').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CASA_PROPRIA').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NOME_PAI').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NOME_MAE').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NOME_CONJUGE').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CPF_CONJUGE').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('RG_CONJUGE').AsString) + ', ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_NASCIMENTO_CONJUGE').AsDateTime)) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('LOCAL_TRABALHO_CONJUGE').AsString) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('LIMITE_CREDITO').AsFloat) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('VENDE_VAREJO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('VENDE_ATACADO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('COBRAR_JUROS').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CONSUMIDOR_FINAL').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NUMERO').AsString) + ', ' +
      IntToStr(QrySQLServer.FieldByName('IDCIDADE').AsInteger) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CONTRIBUINTE').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('STATUS').AsString) +
      ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro inserindo registro ' + IntToStr(Contador) + ': ' + E.Message);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarClientes finalizada. Total registros: ' + IntToStr(Contador));
  Sleep(3000);
end;

procedure TMigrador.MigrarFornecedores;
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
    'CASE WHEN PAR.PAR_TIPOPESSOA = ''F'' THEN ''F�sica'' ELSE ''Jur�dica'' END AS PESSOA, ' +
    'NULL AS SEXO, ' +
    'NULL AS ESTADO_CIVIL, ' +
    'PAR.PAR_RGINSCRICAOESTADUAL AS RG_INSCRICAO, ' +
    'PAR.PAR_CPFCNPJ AS CPF_CNPJ, ' +
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

var
  InsertSQL: string;
  Contador: Integer;
begin
  MemoLog.Lines.Add('Iniciando etapa: MigrarFornecedores...');

  try
    QryFirebird.SQL.Text := 'DELETE FROM FORNECEDORES';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela FORNECEDORES limpa antes da migra��o.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar tabela FORNECEDORES: ' + E.Message);
  end;

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta SQL Server executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);

    MemoLog.Lines.Add('Inserindo fornecedor ' + IntToStr(Contador) + ' - CODIGO: ' + QrySQLServer.FieldByName('CODIGO').AsString);

    InsertSQL :=
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
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_NASCIMENTO').AsDateTime)) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('PESSOA').AsString) + ', ' +
      'NULL, ' +
      'NULL, ' +
      QuotedStr(QrySQLServer.FieldByName('RG_INSCRICAO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('CPF_CNPJ').AsString) + ', ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_INC').AsDateTime)) + ', ' +
      'NULL, ' +
      'NULL, ' +
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_ALT').AsDateTime)) + ', ' +
      'NULL, ' +
      'NULL, ' +
      'NULL, ' +
      'NULL, ' +
      'NULL, ' +
      '0, ' +
      IntToStr(QrySQLServer.FieldByName('IDCIDADE').AsInteger) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NUMERO').AsString) +
      ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro inserindo fornecedor ' + IntToStr(Contador) + ': ' + E.Message);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarFornecedores finalizada. Total registros: ' + IntToStr(Contador));
  Sleep(3000);
end;

procedure TMigrador.MigrarGrupos;
const
  SQL_SELECT =
    'SELECT ' +
    'RIGHT(REPLICATE(''0'', 4) + CAST(IGS_ID AS VARCHAR(4)), 4) AS GRUPO, ' +
    'LEFT(IGS_NOME, 50) AS NOME ' +
    'FROM ITENSGRUPOSUBGRUPO';

var
  InsertSQL: string;
  Contador: Integer;
begin
  MemoLog.Lines.Add('Iniciando etapa: MigrarGrupos...');

  try
    QryFirebird.SQL.Text := 'DELETE FROM GRUPOS';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela GRUPOS limpa antes da migra��o.');
  except
    on E: Exception do
    begin
      MemoLog.Lines.Add('Erro ao limpar tabela GRUPOS: ' + E.Message);
      Exit;
    end;
  end;

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta SQL Server executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);
    MemoLog.Lines.Add('Inserindo grupo ' + IntToStr(Contador) + ' - GRUPO: ' + QrySQLServer.FieldByName('GRUPO').AsString);

    InsertSQL :=
      'INSERT INTO GRUPOS (GRUPO, NOME) VALUES (' +
      QuotedStr(QrySQLServer.FieldByName('GRUPO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NOME').AsString) + ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro inserindo grupo ' + IntToStr(Contador) + ': ' + E.Message);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarGrupos finalizada. Total registros: ' + IntToStr(Contador));
  Sleep(3000);
end;

procedure TMigrador.MigrarProdutosPorFornecedor;
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

var
  InsertSQL: string;
  Contador: Integer;
begin
  MemoLog.Lines.Add('Iniciando etapa: MigrarProdutosPorFornecedor...');

  try
    QryFirebird.SQL.Text := 'DELETE FROM PRODUTOS_POR_FORNECEDOR';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela PRODUTOS_POR_FORNECEDOR limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar PRODUTOS_POR_FORNECEDOR: ' + E.Message);
  end;

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);

    MemoLog.Lines.Add('Inserindo produto por fornecedor ' + IntToStr(Contador) +
                      ' - CODIGO: ' + QrySQLServer.FieldByName('CODIGO').AsString +
                      ', FORNECEDOR: ' + QrySQLServer.FieldByName('FORNECEDOR').AsString);

    InsertSQL :=
      'INSERT INTO PRODUTOS_POR_FORNECEDOR (CODIGO, FORNECEDOR, PADRAO, COD_PROD_FOR) VALUES (' +
      QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('FORNECEDOR').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('PADRAO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('COD_PROD_FOR').AsString) +
      ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro ao inserir registro ' + IntToStr(Contador) + ': ' + E.Message);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarProdutosPorFornecedor finalizada. Total: ' + IntToStr(Contador));
  Sleep(3000);
end;

procedure TMigrador.MigrarProdutos;
const
  SQL_SELECT =
    'SELECT ' +
    '  RIGHT(REPLICATE(''0'', 6) + CAST(I.ITE_ID AS VARCHAR(6)), 6) AS CODIGO, ' +
    '  I.ITE_DESCRICAOPRINCIPAL AS DESCRICAO, ' +
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

var
  InsertSQL: string;
  Contador: Integer;
begin
  MemoLog.Lines.Add('Iniciando etapa: MigrarProdutos...');

  try
    QryFirebird.SQL.Text := 'DELETE FROM PRODUTOS';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela PRODUTOS limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar PRODUTOS: ' + E.Message);
  end;

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);

    MemoLog.Lines.Add('Inserindo produto ' + IntToStr(Contador) +
                      ' - ' + QrySQLServer.FieldByName('CODIGO').AsString + ': ' +
                      QrySQLServer.FieldByName('DESCRICAO').AsString);

    InsertSQL :=
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
      QuotedStr(FormatDateTime('yyyy-mm-dd', QrySQLServer.FieldByName('DATA_ALT').AsDateTime)) + ', ' +
      QrySQLServer.FieldByName('QUANTIDADE_EMBALAGEM').AsString + ', ' +
      QuotedStr(QrySQLServer.FieldByName('NCM').AsString) +
      ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro ao inserir produto ' + IntToStr(Contador) + ': ' + E.Message);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarProdutos finalizada. Total inserido: ' + IntToStr(Contador));
  Sleep(3000);
end;

procedure TMigrador.MigrarProdutosPorEmpresa;
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

var
  InsertSQL: string;
  Contador: Integer;
  Fmt: TFormatSettings;
begin
  MemoLog.Lines.Add('Iniciando etapa: MigrarProdutosPorEmpresa...');

  Fmt := TFormatSettings.Create('en-US');

  try
    QryFirebird.SQL.Text := 'DELETE FROM PRODUTOS_POR_EMPRESA';
    QryFirebird.ExecSQL;
    MemoLog.Lines.Add('Tabela PRODUTOS_POR_EMPRESA limpa.');
  except
    on E: Exception do
      MemoLog.Lines.Add('Erro ao limpar PRODUTOS_POR_EMPRESA: ' + E.Message);
  end;

  QrySQLServer.Close;
  QrySQLServer.SQL.Text := SQL_SELECT;
  QrySQLServer.Open;
  MemoLog.Lines.Add('Consulta executada. Registros encontrados: ' + IntToStr(QrySQLServer.RecordCount));

  Contador := 0;
  while not QrySQLServer.Eof do
  begin
    Inc(Contador);

    MemoLog.Lines.Add('Inserindo produto por empresa ' + IntToStr(Contador) +
                      ' - ' + QrySQLServer.FieldByName('CODIGO').AsString);

    InsertSQL :=
      'INSERT INTO PRODUTOS_POR_EMPRESA (' +
      'CODIGO, SIGLA_EMPRESA, ICMS_TABELA, VALOR_CUSTO_BRUTO, VALOR_CUSTO, ' +
      'MARGEM_LUCRO, VALOR_VENDA, COMISSAO, ESTOQUE, VENDA_ATACADO, ' +
      'VENDA_VAREJO, IDESTOQUE) VALUES (' +
      QuotedStr(QrySQLServer.FieldByName('CODIGO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('SIGLA_EMPRESA').AsString) + ', ' +
      IntToStr(QrySQLServer.FieldByName('ICMS_TABELA').AsInteger) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('VALOR_CUSTO_BRUTO').AsFloat, Fmt) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('VALOR_CUSTO').AsFloat, Fmt) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('MARGEM_LUCRO').AsFloat, Fmt) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('VALOR_VENDA').AsFloat, Fmt) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('COMISSAO').AsFloat, Fmt) + ', ' +
      FloatToStr(QrySQLServer.FieldByName('ESTOQUE').AsFloat, Fmt) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('VENDA_ATACADO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('VENDA_VAREJO').AsString) + ', ' +
      QuotedStr(QrySQLServer.FieldByName('IDESTOQUE').AsString) +
      ')';

    try
      QryFirebird.SQL.Text := InsertSQL;
      QryFirebird.ExecSQL;
    except
      on E: Exception do
        MemoLog.Lines.Add('Erro ao inserir produto por empresa ' + IntToStr(Contador) + ': ' + E.Message + sLineBreak + InsertSQL);
    end;

    QrySQLServer.Next;
  end;

  MemoLog.Lines.Add('Etapa MigrarProdutosPorEmpresa finalizada. Total inserido: ' + IntToStr(Contador));
  Sleep(3000);
end;





end.
