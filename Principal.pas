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
  FireDAC.Phys.FB, FireDAC.Phys.FBDef,
  FireDAC.Phys.MySQL, FireDAC.Phys.MySQLDef,
  Vcl.StdCtrls, Vcl.Buttons,
  FireDAC.Stan.Def, FireDAC.Stan.Pool, System.IniFiles, System.IOUtils, System.StrUtils;

type
  TMigrador = class(TForm)
    btnMigrar: TBitBtn;
    FDConnFirebird: TFDConnection;
    FDConnMySQL: TFDConnection;
    MemoLog: TMemo;
    lblTitle: TLabel;
    lblSubtitle: TLabel;
    FDPhysMySQLDriverLink1: TFDPhysMySQLDriverLink;
    procedure btnMigrarClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
  private
    QryMySQL, QryFirebird: TFDQuery;

    function EstabelecerConexoes: Boolean;
    function ConfigurarConexaoFirebird(Ini: TIniFile): Boolean;
    function ConfigurarConexaoMySQL(Ini: TIniFile): Boolean;

    procedure LimparTabelaDestino(const NomeTabela: string);
    procedure LogMensagem(const Mensagem: string);
    procedure ExecutarConsultaOrigem(const SQL: string);
    function FormatarDataParaSQL(Data: TDateTime): string;
    function FormatFloatParaSQL(Valor: Double): string;

    procedure Limpar;
    procedure MigrarDadosGrupos;
    procedure MigrarDadosMarcas;
    procedure MigrarDadosClientes;
    procedure MigrarDadosProdutos;
    procedure MigrarDadosProdutosPorEmpresa;
    procedure MigrarDadosFornecedores;
    procedure MigrarProdutosPorFornecedor;

  public
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

  MigrarDadosGrupos;
  MigrarDadosMarcas;
  MigrarDadosClientes;
  MigrarDadosProdutos;
  MigrarDadosProdutosPorEmpresa;
  MigrarDadosFornecedores;
  MigrarProdutosPorFornecedor;
end;


procedure TMigrador.FormCreate(Sender: TObject);
begin
  // Configurar o driver MySQL para usar a DLL na pasta do executável
  FDPhysMySQLDriverLink1.VendorLib := 'libmysql.dll';

  QryMySQL := TFDQuery.Create(Self);
  QryMySQL.Connection := FDConnMySQL;

  QryFirebird := TFDQuery.Create(Self);
  QryFirebird.Connection := FDConnFirebird;
end;


procedure TMigrador.FormDestroy(Sender: TObject);
begin
  QryMySQL.Free;
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
    if not ConfigurarConexaoMySQL(Ini) then Exit;

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


function TMigrador.ConfigurarConexaoMySQL(Ini: TIniFile): Boolean;
begin
  Result := False;
  try
    FDConnMySQL.Close;
    FDConnMySQL.Params.Clear;

    FDConnMySQL.DriverName := 'MySQL';
    FDConnMySQL.Params.Add('Server=' + Ini.ReadString('MySQL', 'Server', 'localhost'));
    FDConnMySQL.Params.Add('Database=' + Ini.ReadString('MySQL', 'Database', 'simplesolution'));
    FDConnMySQL.Params.Add('User_Name=' + Ini.ReadString('MySQL', 'User_Name', 'root'));
    FDConnMySQL.Params.Add('Password=' + Ini.ReadString('MySQL', 'Password', ''));
    FDConnMySQL.Params.Add('Port=' + Ini.ReadString('MySQL', 'Port', '3306'));
    FDConnMySQL.Params.Add('CharacterSet=utf8');
    FDConnMySQL.Params.Add('LoginTimeout=5');

    // Adicione estas linhas para resolver o problema de autenticação:
    FDConnMySQL.Params.Add('AuthPlugin=mysql_native_password');
    FDConnMySQL.Params.Add('UseSSL=False');

    FDConnMySQL.Connected := True;
    LogMensagem('Conectado ao MySQL com sucesso!');
    Result := True;
  except
    on E: Exception do
      LogMensagem('Erro ao conectar MySQL: ' + E.Message);
  end;
end;


procedure TMigrador.Limpar;
begin
  LimparTabelaDestino('MARCAS');
  LimparTabelaDestino('GRUPOS');
  LimparTabelaDestino('VENDAS_PRODUTOS');
  LimparTabelaDestino('VENDAS');
  LimparTabelaDestino('VENDASXCFOP');
  LimparTabelaDestino('VENDASXNFE');
  LimparTabelaDestino('VENDASXREFERENCIADA');
  LimparTabelaDestino('VENDASXTRANSPORTE');
  LimparTabelaDestino('VENDASXCONSUMIDOR');
  LimparTabelaDestino('VENDASXENTREGAS');
  LimparTabelaDestino('VENDASXOBSERVACAO');
  LimparTabelaDestino('CONTAS_RECEBER');
  LimparTabelaDestino('MOVCAIXADETALHE');
  LimparTabelaDestino('MOVIMENTOCAIXA');
  LimparTabelaDestino('CLIENTES');
  LimparTabelaDestino('FORNECEDORES');
  LimparTabelaDestino('PRODUTOS_POR_EMPRESA');
  LimparTabelaDestino('PRODUTOS_POR_FORNECEDOR');
  LimparTabelaDestino('PRODUTOS_POR_CODBARRAS');
  LimparTabelaDestino('PRODUTOS');
  LimparTabelaDestino('HISTORICO_ESTOQUE');
end;

procedure TMigrador.LimparTabelaDestino(const NomeTabela: string);
begin
  try
    QryFirebird.SQL.Text := 'DELETE FROM ' + NomeTabela;
    QryFirebird.ExecSQL;
//    LogMensagem('Tabela ' + NomeTabela + ' limpa com sucesso.');
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
  QryMySQL.Close;
  QryMySQL.FetchOptions.Mode := fmAll;
  QryMySQL.FetchOptions.RecsMax := -1;
  QryMySQL.SQL.Text := SQL;
  QryMySQL.Open;
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
  Result := FloatToStr(Valor, Fmt);
end;



procedure TMigrador.MigrarDadosGrupos;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(pg.prodG_CODIGO, 4, ''0'') AS GRUPO, ' +
    'pg.prodG_DESCRICAO AS NOME ' +
    'FROM produto_grupo pg';
begin
  LogMensagem('Iniciando migração de grupos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' + IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO GRUPOS (GRUPO, NOME) VALUES (' +
          QuotedStr(QryMySQL.FieldByName('GRUPO').AsString) + ', ' +
          QuotedStr(QryMySQL.FieldByName('NOME').AsString) + ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir grupo ' + QryMySQL.FieldByName('GRUPO').AsString + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    // INSERIR O 0001 - PADRAO APENAS UMA VEZ NO FINAL
    try
      QryFirebird.SQL.Text :=
        'INSERT INTO GRUPOS (GRUPO, NOME) VALUES (''0001'', ''PADRAO'')';
      QryFirebird.ExecSQL;
      LogMensagem('Grupo padrão 0001 - PADRAO inserido com sucesso');
    except
      on E: Exception do
        LogMensagem('Erro ao inserir grupo padrão 0001: ' + E.Message);
    end;

    LogMensagem('Migração de grupos concluída. Total: ' + IntToStr(QryMySQL.RecordCount + 1)); // +1 para contar o 0001
  except

  end;
end;



procedure TMigrador.MigrarDadosMarcas;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(f.fab_CODIGO, 4, ''0'') AS MARCA, ' +
    'f.fab_NOME AS NOME ' +
    'FROM fabricante_cad f';
begin
  LogMensagem('Iniciando migração de marcas...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' + IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO MARCAS (MARCA, NOME) VALUES (' +
          QuotedStr(QryMySQL.FieldByName('MARCA').AsString) + ', ' +
          QuotedStr(QryMySQL.FieldByName('NOME').AsString) + ')';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir marca ' + QryMySQL.FieldByName('MARCA').AsString + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    // INSERIR O 0001 - PADRAO APENAS UMA VEZ NO FINAL
    try
      QryFirebird.Close;
      QryFirebird.SQL.Text :=
        'INSERT INTO MARCAS (MARCA, NOME) VALUES (''0001'', ''PADRAO'')';
      QryFirebird.ExecSQL;
      LogMensagem('Marca padrão 0001 - PADRAO inserido com sucesso');
    except

    end;

    LogMensagem('Migração de marcas concluída. Total: ' + IntToStr(QryMySQL.RecordCount + 1)); // +1 para contar o 0001
  except
    on E: Exception do
      LogMensagem('Erro durante migração de marcas: ' + E.Message);
  end;
end;



procedure TMigrador.MigrarDadosClientes;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(c.cli_CODIGO, 9, ''0'') AS CODIGO, ' +
    'LEFT(c.cli_NOMERAZAO, 50) as NOME, ' +
    'LEFT(c.cli_APELIDOFANTASIA, 50) as FANTASIA, ' +
    'LEFT(c.cli_LOGRADOURO, 50) as ENDERECO, ' +
    'LEFT(c.cli_COMPLEMENTO, 50) as COMPLEMENTO, ' +
    'LEFT(c.cli_BAIRRO, 30) as BAIRRO, ' +
    'LEFT(cd.cid_NOME, 50) as CIDADE, ' +
    'c.cli_CEP as CEP, ' +
    'c.cli_SEXO as SEXO, ' +
    'CASE WHEN c.cli_FONERESIDENCIAL IS NULL THEN c.cli_FONE1 ELSE c.cli_FONERESIDENCIAL END as TELEFONE01, ' +
    'c.cli_FONE2 as TELEFONE02, ' +
    'c.cli_CELULAR1 as CELULAR, ' +
    'c.cli_FONECONTATO as CONTATO, ' +
    'CASE WHEN c.cli_FJ = ''F'' THEN ''Física'' ELSE ''Jurídica'' END as PESSOA, ' +
    'c.cli_CPFCNPJ as CPF_CNPJ, ' +
    'c.cli_RGIE as RG_INSCRICAO, ' +
    'LEFT(c.cli_EMAIL, 50) as EMAIL, ' +
    'LEFT(c.cli_EMAILNFE, 50) as EMAIL_NFE, ' +
    '''N'' as VENDA_CONVENIO, ' +
    '''S'' as EMITE_CARTA_COBRANCA, ' +
    '''N'' as EMITE_ALERTA, ' +
    '''S'' as CASA_PROPRIA, ' +
    '''S'' as VENDE_VAREJO, ' +
    '''N'' as VENDE_ATACADO, ' +
    'CASE WHEN c.cli_FJ = ''F'' THEN ''S'' ELSE ''N'' END as CONSUMIDOR_FINAL, ' +
    'LEFT(c.cli_NRO, 30) as NUMERO, ' +
    'cd.cid_CODIBGE as IDCIDADE, ' +
    'CASE WHEN c.cli_ATIVO = ''S'' THEN ''S'' ELSE ''N'' END as STATUS, ' +
    'CASE WHEN c.cli_FJ = ''F'' THEN ''N'' ELSE ''S'' END as CONTRIBUINTE ' +
    'FROM cliente_cad c ' +
    'LEFT JOIN cidade_cad cd ON cd.cid_ID = c.cli_CIDADE_ID ' +
    'WHERE c.cli_CODIGO IS NOT NULL';

  function NullToStr(Field: TField; Default: string = ''): string;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsString;
  end;

  function NullToInt(Field: TField; Default: Integer = 0): Integer;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsInteger;
  end;

  function CorrigirSexo(S: string): string;
  begin
    if UpperCase(Trim(S)) = 'M' then
      Result := 'Masculino'
    else
      Result := 'Feminino';
  end;

begin
  LogMensagem('Iniciando migração de clientes...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' + IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO CLIENTES (' +
          'CODIGO, NOME, FANTASIA, ENDERECO, COMPLEMENTO, BAIRRO, CIDADE, CEP, SEXO, ' +
          'TELEFONE01, TELEFONE02, CELULAR, CONTATO, PESSOA, CPF_CNPJ, RG_INSCRICAO, ' +
          'EMAIL, EMAIL_NFE, VENDA_CONVENIO, EMITE_CARTA_COBRANCA, EMITE_ALERTA, ' +
          'CASA_PROPRIA, VENDE_VAREJO, VENDE_ATACADO, CONSUMIDOR_FINAL, NUMERO, IDCIDADE, STATUS, CONTRIBUINTE, IDGRUPO' +
          ') VALUES (' +
          ':CODIGO, :NOME, :FANTASIA, :ENDERECO, :COMPLEMENTO, :BAIRRO, :CIDADE, :CEP, :SEXO, ' +
          ':TELEFONE01, :TELEFONE02, :CELULAR, :CONTATO, :PESSOA, :CPF_CNPJ, :RG_INSCRICAO, ' +
          ':EMAIL, :EMAIL_NFE, :VENDA_CONVENIO, :EMITE_CARTA_COBRANCA, :EMITE_ALERTA, ' +
          ':CASA_PROPRIA, :VENDE_VAREJO, :VENDE_ATACADO, :CONSUMIDOR_FINAL, :NUMERO, :IDCIDADE, :STATUS, :CONTRIBUINTE, :IDGRUPO)';

        QryFirebird.ParamByName('CODIGO').AsString := NullToStr(QryMySQL.FieldByName('CODIGO'));
        QryFirebird.ParamByName('NOME').AsString := NullToStr(QryMySQL.FieldByName('NOME'));
        QryFirebird.ParamByName('FANTASIA').AsString := NullToStr(QryMySQL.FieldByName('FANTASIA'));
        QryFirebird.ParamByName('ENDERECO').AsString := NullToStr(QryMySQL.FieldByName('ENDERECO'));
        QryFirebird.ParamByName('COMPLEMENTO').AsString := NullToStr(QryMySQL.FieldByName('COMPLEMENTO'));
        QryFirebird.ParamByName('BAIRRO').AsString := NullToStr(QryMySQL.FieldByName('BAIRRO'));
        QryFirebird.ParamByName('CIDADE').AsString := NullToStr(QryMySQL.FieldByName('CIDADE'));
        QryFirebird.ParamByName('CEP').AsString := NullToStr(QryMySQL.FieldByName('CEP'));
        QryFirebird.ParamByName('SEXO').AsString := CorrigirSexo(NullToStr(QryMySQL.FieldByName('SEXO')));
        QryFirebird.ParamByName('TELEFONE01').AsString := NullToStr(QryMySQL.FieldByName('TELEFONE01'));
        QryFirebird.ParamByName('TELEFONE02').AsString := NullToStr(QryMySQL.FieldByName('TELEFONE02'));
        QryFirebird.ParamByName('CELULAR').AsString := NullToStr(QryMySQL.FieldByName('CELULAR'));
        QryFirebird.ParamByName('CONTATO').AsString := NullToStr(QryMySQL.FieldByName('CONTATO'));
        QryFirebird.ParamByName('PESSOA').AsString := NullToStr(QryMySQL.FieldByName('PESSOA'));
        QryFirebird.ParamByName('CPF_CNPJ').AsString := NullToStr(QryMySQL.FieldByName('CPF_CNPJ'));
        QryFirebird.ParamByName('RG_INSCRICAO').AsString := NullToStr(QryMySQL.FieldByName('RG_INSCRICAO'));
        QryFirebird.ParamByName('EMAIL').AsString := NullToStr(QryMySQL.FieldByName('EMAIL'));
        QryFirebird.ParamByName('EMAIL_NFE').AsString := NullToStr(QryMySQL.FieldByName('EMAIL_NFE'));
        QryFirebird.ParamByName('VENDA_CONVENIO').AsString := NullToStr(QryMySQL.FieldByName('VENDA_CONVENIO'));
        QryFirebird.ParamByName('EMITE_CARTA_COBRANCA').AsString := NullToStr(QryMySQL.FieldByName('EMITE_CARTA_COBRANCA'));
        QryFirebird.ParamByName('EMITE_ALERTA').AsString := NullToStr(QryMySQL.FieldByName('EMITE_ALERTA'));
        QryFirebird.ParamByName('CASA_PROPRIA').AsString := NullToStr(QryMySQL.FieldByName('CASA_PROPRIA'));
        QryFirebird.ParamByName('VENDE_VAREJO').AsString := NullToStr(QryMySQL.FieldByName('VENDE_VAREJO'));
        QryFirebird.ParamByName('VENDE_ATACADO').AsString := NullToStr(QryMySQL.FieldByName('VENDE_ATACADO'));
        QryFirebird.ParamByName('CONSUMIDOR_FINAL').AsString := NullToStr(QryMySQL.FieldByName('CONSUMIDOR_FINAL'));
        QryFirebird.ParamByName('NUMERO').AsString := NullToStr(QryMySQL.FieldByName('NUMERO'));
        QryFirebird.ParamByName('IDCIDADE').AsInteger := NullToInt(QryMySQL.FieldByName('IDCIDADE'));
        QryFirebird.ParamByName('STATUS').AsString := NullToStr(QryMySQL.FieldByName('STATUS'));
        QryFirebird.ParamByName('CONTRIBUINTE').AsString := NullToStr(QryMySQL.FieldByName('CONTRIBUINTE'));
        QryFirebird.ParamByName('IDGRUPO').AsString := '001';

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir cliente ' + NullToStr(QryMySQL.FieldByName('CODIGO')) + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    LogMensagem('Migração de clientes concluída. Total: ' + IntToStr(QryMySQL.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de clientes: ' + E.Message);
  end;
end;



procedure TMigrador.MigrarDadosProdutos;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(p.prod_ID, 6, ''0'') AS CODIGO, ' +
    'LEFT(p.prod_DESCRICAO, 100) AS DESCRICAO, ' +
    'LEFT(p.prod_REFERENCIA, 30) AS CODIGO_ANTERIOR, ' +
    '''0'' AS CLAS_ORIGEM, ' +
    '''101'' AS CLAS_DESPESA, ' +
    '''SEM COR'' AS COR, ' +
    'LPAD(f.fab_CODIGO, 4, ''0'') AS MARCA, ' +
    '''0001'' AS DPTO_PRODUTO, ' +
    'LPAD(pg.prodG_CODIGO, 4, ''0'') AS GRUPO, ' +
    'LEFT(md.med_ABREVIATURA, 4) AS UMEDIDA, ' +
    'LEFT(PROD_ativo, 1) AS STATUS, ' +
    '''N'' AS MONTAGEM, ' +
    '''N'' AS ENTREGA, ' +
    '''S'' AS BAIXA_ESTOQUE, ' +
    '''P'' AS TIPO, ' +
    '''N'' AS COMPOSICAO, ' +
    '''1'' AS QUANTIDADE_EMBALAGEM, ' +
    'LEFT(pn.prodNCM_NCM, 10) AS NCM, ' +
    'p.prod_PESOLIQUIDO AS PESO_LIQUIDO, ' +
    'p.prod_PESOBRUTO AS PESO_BRUTO, ' +
    'p.prod_LARGURA AS LARGURA, ' +
    'p.prod_ALTURA AS ALTURA, ' +
    'p.prod_COMPRIMENTO AS COMPRIMENTO, ' +
    'LEFT(p.prod_codbarras, 13) AS CODIGO_BARRA ' +
    'FROM produto_cad p ' +
    'LEFT JOIN produto_ncm pn ON pn.prodNCM_ID = p.prod_ID ' +
    'LEFT JOIN medida_cad md ON md.med_ID = p.prod_UNIDADEVENDAID ' +
    'LEFT JOIN produto_grupo pg ON pg.prodG_ID = p.prod_GRUPO ' +
    'LEFT JOIN fabricante_cad f ON f.fab_CODIGO = p.prod_FABRICANTEID ' +
    'WHERE p.prod_ID IS NOT NULL';

  function NullToStr(Field: TField; Default: string = ''): string;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsString;
  end;

  function NullToFloat(Field: TField; Default: Double = 0): Double;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsFloat;
  end;

  function CorrigirMedida(const Valor: string): string;
  begin
    Result := Trim(Valor);
    if Result = '' then
      Result := 'UN';
    if Length(Result) > 4 then
      Result := Copy(Result, 1, 4);
  end;

  function CorrigirGrupo(const Valor: string): string;
  begin
    Result := Trim(Valor);
    if Result = '' then
      Result := '0999';
    if Length(Result) > 15 then
      Result := Copy(Result, 1, 15);
  end;

  function CorrigirMarca(const Valor: string): string;
  begin
    Result := Trim(Valor);
    if Result = '' then
      Result := '0999';
    if Length(Result) > 15 then
      Result := Copy(Result, 1, 15);
  end;

  procedure GarantirMedida(const Medida: string);
  var
    LMedida: string;
  begin
    if Trim(Medida) = '' then
      LMedida := 'UN'
    else
      LMedida := Copy(Trim(Medida), 1, 4);

    QryFirebird.Close;
    QryFirebird.SQL.Text :=
      'SELECT COUNT(*) AS QTD FROM MEDIDAS WHERE UNIDADE_MEDIDA = :M';
    QryFirebird.ParamByName('M').AsString := LMedida;
    QryFirebird.Open;

    if QryFirebird.FieldByName('QTD').AsInteger = 0 then
    begin
      QryFirebird.Close;
      QryFirebird.SQL.Text :=
        'INSERT INTO MEDIDAS (UNIDADE_MEDIDA, NOME) VALUES (:M, :N)';
      QryFirebird.ParamByName('M').AsString := LMedida;
      QryFirebird.ParamByName('N').AsString := LMedida;
      QryFirebird.ExecSQL;
    end;

    QryFirebird.Close;
  end;

  procedure GarantirGrupo(const Grupo: string);
  begin
    QryFirebird.Close;
    QryFirebird.SQL.Text := 'SELECT COUNT(*) AS QTD FROM GRUPOS WHERE GRUPO = :G';
    QryFirebird.ParamByName('G').AsString := Grupo;
    QryFirebird.Open;

    if QryFirebird.FieldByName('QTD').AsInteger = 0 then
    begin
      QryFirebird.Close;
      QryFirebird.SQL.Text := 'INSERT INTO GRUPOS (GRUPO, NOME) VALUES (:G, :N)';
      QryFirebird.ParamByName('G').AsString := Grupo;
      QryFirebird.ParamByName('N').AsString := 'NAO DEFINIDO';
      QryFirebird.ExecSQL;
    end;

    QryFirebird.Close;
  end;

  procedure GarantirMarca(const Marca: string);
  begin
    QryFirebird.Close;
    QryFirebird.SQL.Text := 'SELECT COUNT(*) AS QTD FROM MARCAS WHERE MARCA = :M';
    QryFirebird.ParamByName('M').AsString := Marca;
    QryFirebird.Open;

    if QryFirebird.FieldByName('QTD').AsInteger = 0 then
    begin
      QryFirebird.Close;
      QryFirebird.SQL.Text := 'INSERT INTO MARCAS (MARCA, NOME) VALUES (:M, :N)';
      QryFirebird.ParamByName('M').AsString := Marca;
      QryFirebird.ParamByName('N').AsString := 'NAO DEFINIDO';
      QryFirebird.ExecSQL;
    end;

    QryFirebird.Close;
  end;

var
  Unidade, Grupo, Marca: string;
begin
  LogMensagem('Iniciando migração de produtos...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' +
      IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        Unidade := CorrigirMedida(NullToStr(QryMySQL.FieldByName('UMEDIDA')));
        GarantirMedida(Unidade);

        Grupo := CorrigirGrupo(NullToStr(QryMySQL.FieldByName('GRUPO')));
        GarantirGrupo(Grupo);

        Marca := CorrigirMarca(NullToStr(QryMySQL.FieldByName('MARCA')));
        GarantirMarca(Marca);

        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO PRODUTOS (' +
          'CODIGO, DESCRICAO, CODIGO_ANTERIOR, CLAS_ORIGEM, CLAS_DESPESA, COR, MARCA, ' +
          'DPTO_PRODUTO, GRUPO, UMEDIDA, STATUS, MONTAGEM, ENTREGA, BAIXA_ESTOQUE, TIPO, ' +
          'COMPOSICAO, QUANTIDADE_EMBALAGEM, NCM, ' +
          'PESO_LIQUIDO, PESO_BRUTO, LADO_A_COMPRIMENTO, LADO_B_LARGURA, LADO_C_ALTURA, ' +
          'CODIGO_BARRA' +
          ') VALUES (' +
          ':CODIGO, :DESCRICAO, :CODIGO_ANTERIOR, :CLAS_ORIGEM, :CLAS_DESPESA, :COR, :MARCA, ' +
          ':DPTO_PRODUTO, :GRUPO, :UMEDIDA, :STATUS, :MONTAGEM, :ENTREGA, :BAIXA_ESTOQUE, :TIPO, ' +
          ':COMPOSICAO, :QUANTIDADE_EMBALAGEM, :NCM, ' +
          ':PESO_LIQUIDO, :PESO_BRUTO, :LADO_A_COMPRIMENTO, :LADO_B_LARGURA, :LADO_C_ALTURA, ' +
          ':CODIGO_BARRA)';

        QryFirebird.ParamByName('CODIGO').AsString := NullToStr(QryMySQL.FieldByName('CODIGO'));
        QryFirebird.ParamByName('DESCRICAO').AsString := NullToStr(QryMySQL.FieldByName('DESCRICAO'));
        QryFirebird.ParamByName('CODIGO_ANTERIOR').AsString := NullToStr(QryMySQL.FieldByName('CODIGO_ANTERIOR'));
        QryFirebird.ParamByName('CLAS_ORIGEM').AsString := NullToStr(QryMySQL.FieldByName('CLAS_ORIGEM'));
        QryFirebird.ParamByName('CLAS_DESPESA').AsString := NullToStr(QryMySQL.FieldByName('CLAS_DESPESA'));
        QryFirebird.ParamByName('COR').AsString := NullToStr(QryMySQL.FieldByName('COR'));
        QryFirebird.ParamByName('MARCA').AsString := Marca;
        QryFirebird.ParamByName('DPTO_PRODUTO').AsString := NullToStr(QryMySQL.FieldByName('DPTO_PRODUTO'));
        QryFirebird.ParamByName('GRUPO').AsString := Grupo;
        QryFirebird.ParamByName('UMEDIDA').AsString := Unidade;
        QryFirebird.ParamByName('STATUS').AsString := NullToStr(QryMySQL.FieldByName('STATUS'));
        QryFirebird.ParamByName('MONTAGEM').AsString := NullToStr(QryMySQL.FieldByName('MONTAGEM'));
        QryFirebird.ParamByName('ENTREGA').AsString := NullToStr(QryMySQL.FieldByName('ENTREGA'));
        QryFirebird.ParamByName('BAIXA_ESTOQUE').AsString := NullToStr(QryMySQL.FieldByName('BAIXA_ESTOQUE'));
        QryFirebird.ParamByName('TIPO').AsString := NullToStr(QryMySQL.FieldByName('TIPO'));
        QryFirebird.ParamByName('COMPOSICAO').AsString := NullToStr(QryMySQL.FieldByName('COMPOSICAO'));
        QryFirebird.ParamByName('QUANTIDADE_EMBALAGEM').AsInteger :=
          StrToIntDef(NullToStr(QryMySQL.FieldByName('QUANTIDADE_EMBALAGEM'), '1'), 1);
        QryFirebird.ParamByName('NCM').AsString := NullToStr(QryMySQL.FieldByName('NCM'));

        QryFirebird.ParamByName('PESO_LIQUIDO').AsFloat := NullToFloat(QryMySQL.FieldByName('PESO_LIQUIDO'));
        QryFirebird.ParamByName('PESO_BRUTO').AsFloat := NullToFloat(QryMySQL.FieldByName('PESO_BRUTO'));
        QryFirebird.ParamByName('LADO_A_COMPRIMENTO').AsFloat := NullToFloat(QryMySQL.FieldByName('COMPRIMENTO'));
        QryFirebird.ParamByName('LADO_B_LARGURA').AsFloat := NullToFloat(QryMySQL.FieldByName('LARGURA'));
        QryFirebird.ParamByName('LADO_C_ALTURA').AsFloat := NullToFloat(QryMySQL.FieldByName('ALTURA'));

        QryFirebird.ParamByName('CODIGO_BARRA').AsString := NullToStr(QryMySQL.FieldByName('CODIGO_BARRA'));

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto ' +
            NullToStr(QryMySQL.FieldByName('CODIGO')) + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    LogMensagem('Migração de produtos concluída. Total: ' +
      IntToStr(QryMySQL.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos: ' + E.Message);
  end;
end;



procedure TMigrador.MigrarDadosProdutosPorEmpresa;
var
  Codigo: string;
  ValorVenda, MargemLucro, ValorVendaAta, MargemLucroAta, Estoque, ValorCusto, ValorCustoBruto: Double;
begin
  MemoLog.Lines.Add(TimeToStr(Now) + ' - Iniciando importação de produtos por empresa...');

  QryFirebird.Close;
  QryFirebird.SQL.Text := 'SET GENERATOR GEN_HISTORICO_ESTOQUE_ID TO 0';
  QryFirebird.ExecSQL;

  QryMySQL.Close;
  QryMySQL.SQL.Text :=
    'SELECT ' +
    'LPAD(p.prod_ID, 6, ''0'') AS CODIGO, ' +
    '''001'' AS SIGLA_EMPRESA, ' +
    '''1'' AS ICMS_TABELA, ' +
    '''N'' AS VENDA_ATACADO, ' +
    'pp1.prodPr_VALOR AS VALOR_VENDA, ' +
    'pp1.prodPr_PERCENT AS MARGEM_LUCRO, ' +
    'pp2.prodPr_VALOR AS VALOR_VENDA_ATA, ' +
    'pp2.prodPr_PERCENT AS MARGEM_LUCRO_ATA, ' +
    '''S'' AS VENDA_VAREJO, ' +
    '''001'' AS IDESTOQUE, ' +
    'p.prod_ESTOQUEREAL AS ESTOQUE, ' +
    'p.prod_custoproduto AS VALOR_CUSTO, ' +
    'p.prod_custoproduto AS VALOR_CUSTO_BRUTO ' +
    'FROM produto_cad p ' +
    'LEFT JOIN produto_preco pp1 ON pp1.prodPr_PRODUTO = p.prod_ID AND pp1.prodPr_PRODUTOPRECOTIPOID = 1 ' +
    'LEFT JOIN produto_preco pp2 ON pp2.prodPr_PRODUTO = p.prod_ID AND pp2.prodPr_PRODUTOPRECOTIPOID = 2 ' +
    'WHERE p.prod_ID IS NOT NULL ' +
    'ORDER BY p.prod_ID;';
  QryMySQL.Open;

  while not QryMySQL.Eof do
  begin
    try
      Codigo := QryMySQL.FieldByName('CODIGO').AsString;

      ValorVenda      := QryMySQL.FieldByName('VALOR_VENDA').AsFloat;
      MargemLucro     := QryMySQL.FieldByName('MARGEM_LUCRO').AsFloat;
      ValorVendaAta   := QryMySQL.FieldByName('VALOR_VENDA_ATA').AsFloat;
      MargemLucroAta  := QryMySQL.FieldByName('MARGEM_LUCRO_ATA').AsFloat;
      Estoque         := QryMySQL.FieldByName('ESTOQUE').AsFloat;
      ValorCusto      := QryMySQL.FieldByName('VALOR_CUSTO').AsFloat;
      ValorCustoBruto := QryMySQL.FieldByName('VALOR_CUSTO_BRUTO').AsFloat;

      QryFirebird.Close;
      QryFirebird.SQL.Text :=
        'INSERT INTO PRODUTOS_POR_EMPRESA ' +
        '(CODIGO, SIGLA_EMPRESA, ICMS_TABELA, VENDA_ATACADO, VALOR_VENDA, VALOR_VENDA_ATA, ' +
        'MARGEM_LUCRO, MARGEM_LUCRO_ATA, VENDA_VAREJO, IDESTOQUE, ESTOQUE, VALOR_CUSTO, VALOR_CUSTO_BRUTO) ' +
        'VALUES (:CODIGO, :SIGLA_EMPRESA, :ICMS_TABELA, :VENDA_ATACADO, :VALOR_VENDA, :VALOR_VENDA_ATA, ' +
        ':MARGEM_LUCRO, :MARGEM_LUCRO_ATA, :VENDA_VAREJO, :IDESTOQUE, :ESTOQUE, :VALOR_CUSTO, :VALOR_CUSTO_BRUTO)';

      QryFirebird.ParamByName('CODIGO').AsString        := Codigo;
      QryFirebird.ParamByName('SIGLA_EMPRESA').AsString := QryMySQL.FieldByName('SIGLA_EMPRESA').AsString;
      QryFirebird.ParamByName('ICMS_TABELA').AsString   := QryMySQL.FieldByName('ICMS_TABELA').AsString;
      QryFirebird.ParamByName('VENDA_ATACADO').AsString := QryMySQL.FieldByName('VENDA_ATACADO').AsString;
      QryFirebird.ParamByName('VALOR_VENDA').AsFloat    := ValorVenda;
      QryFirebird.ParamByName('MARGEM_LUCRO').AsFloat   := MargemLucro;
      QryFirebird.ParamByName('VALOR_VENDA_ATA').AsFloat := ValorVendaAta;
      QryFirebird.ParamByName('MARGEM_LUCRO_ATA').AsFloat := MargemLucroAta;
      QryFirebird.ParamByName('VENDA_VAREJO').AsString  := QryMySQL.FieldByName('VENDA_VAREJO').AsString;
      QryFirebird.ParamByName('IDESTOQUE').AsString     := QryMySQL.FieldByName('IDESTOQUE').AsString;
      QryFirebird.ParamByName('ESTOQUE').AsFloat        := Estoque;   // Estoque
      QryFirebird.ParamByName('VALOR_CUSTO').AsFloat    := ValorCusto;
      QryFirebird.ParamByName('VALOR_CUSTO_BRUTO').AsFloat := ValorCustoBruto;

      QryFirebird.ExecSQL;


      // Historico
      QryFirebird.Close;
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
        QuotedStr(Codigo) + ', ' +
        '''Migração'', ' +
        FormatFloatParaSQL(Estoque) +
        ')';

      QryFirebird.ExecSQL;

    except
      on E: Exception do
        MemoLog.Lines.Add(TimeToStr(Now) + ' - Erro ao inserir produto por empresa ' + Codigo + ': ' + E.Message);
    end;

    QryMySQL.Next;
  end;

  MemoLog.Lines.Add(TimeToStr(Now) + ' - Importação de produtos por empresa finalizada.');
end;



procedure TMigrador.MigrarDadosFornecedores;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(f.forn_CODIGO, 6, ''0'') AS CODIGO, ' +
    'LEFT(f.forn_NOMERAZAO, 50) AS NOME, ' +
    'LEFT(f.forn_APELIDOFANTASIA, 50) AS FANTASIA, ' +
    'LEFT(f.forn_LOGRADOURO, 50) AS ENDERECO, ' +
    'LEFT(f.forn_COMPLEMENTO, 50) AS COMPLEMENTO, ' +
    'LEFT(f.forn_BAIRRO, 30) AS BAIRRO, ' +
    'f.forn_CEP AS CEP, ' +
    'f.forn_CXPOSTAL AS CXPOSTAL, ' +
    'f.forn_NUMERO AS NUMERO, ' +
    'f.forn_CPFCNPJ AS CPF_CNPJ, ' +
    'f.forn_RGIE AS RG_INSCRICAO, ' +
    'f.forn_CONTATO AS CONTATO, ' +
    'f.forn_TELEFONE1 AS TELEFONE01, ' +
    'f.forn_TELEFONE2 AS TELEFONE02, ' +
    'f.forn_CELULAR AS CELULAR, ' +
    'f.forn_FAX AS FAX, ' +
    'f.forn_EMAIL AS EMAIL, ' +
    'f.forn_OBS AS OBSERVACOES, ' +
    'f.forn_ATIVO AS STATUS, ' +
    'f.forn_FJ AS PESSOA, ' +
    'f.forn_IEUF AS ESTADO, ' +
    'c.cid_NOME AS tmpCidade, ' +
    'f.forn_VRMINIMOCOMPRAS AS VALOR_MINIMO_COMPRA, ' +
    'f.forn_PRAZOVCTODIAS AS PRAZO_ENTREGA, ' +
    'f.forn_DTCAD AS DATA_INC, ' +
    'f.forn_USUARIOCAD AS USU_INC, ' +
    'f.forn_DTALT AS DATA_ALT, ' +
    'f.forn_USUARIOALT AS USU_ALT ' +
    'FROM fornecedor_cad f ' +
    'LEFT JOIN cidade_cad c ON c.cid_ID = f.forn_CIDADEID ' +
    'WHERE f.forn_CODIGO IS NOT NULL';

  function NullToStr(Field: TField; Default: string = ''): string;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsString;
  end;

  function NullToFloat(Field: TField; Default: Double = 0): Double;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsFloat;
  end;

  function NullToInt(Field: TField; Default: Integer = 0): Integer;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsInteger;
  end;

  function TruncStr(const Value: string; MaxLen: Integer): string;
  begin
    if Length(Value) > MaxLen then
      Result := Copy(Value, 1, MaxLen)
    else
      Result := Value;
  end;

begin
  LogMensagem('Iniciando migração de fornecedores...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' + IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO FORNECEDORES (' +
          'CODIGO, NOME, CODIGO_ANTERIOR, FANTASIA, ENDERECO, COMPLEMENTO, BAIRRO, CIDADE, CEP, CXPOSTAL, ' +
          'NUMERO, CPF_CNPJ, RG_INSCRICAO, CONTATO, TELEFONE01, TELEFONE02, CELULAR, FAX, EMAIL, OBSERVACOES, ' +
          'STATUS, PESSOA, ESTADO, VALOR_MINIMO_COMPRA, PRAZO_ENTREGA, DATA_INC, USU_INC, DATA_ALT, USU_ALT' +
          ') VALUES (' +
          ':CODIGO, :NOME, :CODIGO_ANTERIOR, :FANTASIA, :ENDERECO, :COMPLEMENTO, :BAIRRO, :CIDADE, :CEP, :CXPOSTAL, ' +
          ':NUMERO, :CPF_CNPJ, :RG_INSCRICAO, :CONTATO, :TELEFONE01, :TELEFONE02, :CELULAR, :FAX, :EMAIL, :OBSERVACOES, ' +
          ':STATUS, :PESSOA, :ESTADO, :VALOR_MINIMO_COMPRA, :PRAZO_ENTREGA, :DATA_INC, :USU_INC, :DATA_ALT, :USU_ALT)';

        QryFirebird.ParamByName('CODIGO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CODIGO')), 6);
        QryFirebird.ParamByName('NOME').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('NOME')), 50);
        QryFirebird.ParamByName('CODIGO_ANTERIOR').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CODIGO')), 15);
        QryFirebird.ParamByName('FANTASIA').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('FANTASIA')), 50);
        QryFirebird.ParamByName('ENDERECO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('ENDERECO')), 50);
        QryFirebird.ParamByName('COMPLEMENTO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('COMPLEMENTO')), 50);
        QryFirebird.ParamByName('BAIRRO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('BAIRRO')), 30);
        QryFirebird.ParamByName('CIDADE').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('tmpCidade')), 30);
        QryFirebird.ParamByName('CEP').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CEP')), 9);
        QryFirebird.ParamByName('CXPOSTAL').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CXPOSTAL')), 10);
        QryFirebird.ParamByName('NUMERO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('NUMERO')), 10);
        QryFirebird.ParamByName('CPF_CNPJ').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CPF_CNPJ')), 18);
        QryFirebird.ParamByName('RG_INSCRICAO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('RG_INSCRICAO')), 20);
        QryFirebird.ParamByName('CONTATO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CONTATO')), 200);
        QryFirebird.ParamByName('TELEFONE01').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('TELEFONE01')), 15);
        QryFirebird.ParamByName('TELEFONE02').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('TELEFONE02')), 15);
        QryFirebird.ParamByName('CELULAR').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CELULAR')), 15);
        QryFirebird.ParamByName('FAX').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('FAX')), 15);
        QryFirebird.ParamByName('EMAIL').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('EMAIL')), 200);
        QryFirebird.ParamByName('OBSERVACOES').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('OBSERVACOES')), 500);
        QryFirebird.ParamByName('STATUS').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('STATUS')), 1);
        QryFirebird.ParamByName('PESSOA').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('PESSOA')), 8);
        QryFirebird.ParamByName('ESTADO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('ESTADO')), 2);
        QryFirebird.ParamByName('VALOR_MINIMO_COMPRA').AsFloat := NullToFloat(QryMySQL.FieldByName('VALOR_MINIMO_COMPRA'));
        QryFirebird.ParamByName('PRAZO_ENTREGA').AsInteger := NullToInt(QryMySQL.FieldByName('PRAZO_ENTREGA'));
        QryFirebird.ParamByName('DATA_INC').AsDateTime := QryMySQL.FieldByName('DATA_INC').AsDateTime;
        QryFirebird.ParamByName('USU_INC').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('USU_INC')), 15);
        QryFirebird.ParamByName('DATA_ALT').AsDateTime := QryMySQL.FieldByName('DATA_ALT').AsDateTime;
        QryFirebird.ParamByName('USU_ALT').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('USU_ALT')), 15);

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir fornecedor ' + NullToStr(QryMySQL.FieldByName('CODIGO')) + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    LogMensagem('Migração de fornecedores concluída. Total: ' + IntToStr(QryMySQL.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de fornecedores: ' + E.Message);
  end;
end;



procedure TMigrador.MigrarProdutosPorFornecedor;
const
  SQL_SELECT =
    'SELECT ' +
    'LPAD(p.prodCodF_PRODUTO, 6, ''0'') AS CODIGO, ' +
    'LPAD(p.prodCodF_FORNECEDOR, 6, ''0'') AS FORNECEDOR, ' +
    '''S'' AS PADRAO, ' +
    'LEFT(p.prodCodF_PRODUTOREFERENCIA, 60) AS COD_PROD_FOR ' +
    'FROM produto_codigo_fornecedor p ' +
    'INNER JOIN (' +
    '   SELECT prodCodF_PRODUTO, prodCodF_FORNECEDOR, MAX(prodCodF_DTCAD) AS ULTIMA_DATA ' +
    '   FROM produto_codigo_fornecedor ' +
    '   GROUP BY prodCodF_PRODUTO, prodCodF_FORNECEDOR' +
    ') ultimos ON p.prodCodF_PRODUTO = ultimos.prodCodF_PRODUTO ' +
    'AND p.prodCodF_FORNECEDOR = ultimos.prodCodF_FORNECEDOR ' +
    'AND p.prodCodF_DTCAD = ultimos.ULTIMA_DATA ' +
    'WHERE p.prodCodF_PRODUTO IS NOT NULL';

  function NullToStr(Field: TField; Default: string = ''): string;
  begin
    if Field.IsNull then
      Result := Default
    else
      Result := Field.AsString;
  end;

  function TruncStr(const Value: string; MaxLen: Integer): string;
  begin
    if Length(Value) > MaxLen then
      Result := Copy(Value, 1, MaxLen)
    else
      Result := Value;
  end;

begin
  LogMensagem('Iniciando migração de produtos por fornecedor...');

  try
    ExecutarConsultaOrigem(SQL_SELECT);
    LogMensagem('Consulta MySQL executada. Registros encontrados: ' + IntToStr(QryMySQL.RecordCount));

    while not QryMySQL.Eof do
    begin
      try
        QryFirebird.Close;
        QryFirebird.SQL.Text :=
          'INSERT INTO PRODUTOS_POR_FORNECEDOR (' +
          'CODIGO, FORNECEDOR, PADRAO, COD_PROD_FOR' +
          ') VALUES (' +
          ':CODIGO, :FORNECEDOR, :PADRAO, :COD_PROD_FOR)';

        QryFirebird.ParamByName('CODIGO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('CODIGO')), 6);
        QryFirebird.ParamByName('FORNECEDOR').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('FORNECEDOR')), 6);
        QryFirebird.ParamByName('PADRAO').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('PADRAO')), 1);
        QryFirebird.ParamByName('COD_PROD_FOR').AsString := TruncStr(NullToStr(QryMySQL.FieldByName('COD_PROD_FOR')), 60);

        QryFirebird.ExecSQL;
      except
        on E: Exception do
          LogMensagem('Erro ao inserir produto ' + NullToStr(QryMySQL.FieldByName('CODIGO')) +
            ' do fornecedor ' + NullToStr(QryMySQL.FieldByName('FORNECEDOR')) + ': ' + E.Message);
      end;

      QryMySQL.Next;
    end;

    LogMensagem('Migração de produtos por fornecedor concluída. Total: ' + IntToStr(QryMySQL.RecordCount));
  except
    on E: Exception do
      LogMensagem('Erro durante migração de produtos por fornecedor: ' + E.Message);
  end;
end;


























end.
