program MigraFireMySql;

uses
  Vcl.Forms,
  Principal in 'Principal.pas' {Migrador};

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TMigrador, Migrador);
  Application.Run;
end.
