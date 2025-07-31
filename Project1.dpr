program Project1;

uses
  Vcl.Forms,
  Unit1 in 'Unit1.pas' {Migrador};

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TMigrador, Migrador);
  Application.Run;
end.
