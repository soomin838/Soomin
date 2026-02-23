[Setup]
AppId={{8C8A0F3A-DC89-4D6D-B9E5-9D8CD5A9D981}
AppName=RezeroAgent
AppVersion=1.0.0
DefaultDirName={autopf}\RezeroAgent
DefaultGroupName=RezeroAgent
OutputDir=..\..\dist
OutputBaseFilename=RezeroAgentInstaller
Compression=lzma
SolidCompression=yes
ArchitecturesAllowed=arm64 x64compatible
ArchitecturesInstallIn64BitMode=arm64 x64compatible
CloseApplications=yes
CloseApplicationsFilter=RezeroAgent.exe
RestartApplications=no

[Files]
Source: "..\..\dist\RezeroAgent.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\RezeroAgent"; Filename: "{app}\RezeroAgent.exe"
Name: "{group}\Uninstall RezeroAgent"; Filename: "{uninstallexe}"
Name: "{autodesktop}\RezeroAgent"; Filename: "{app}\RezeroAgent.exe"

[Run]
Filename: "{app}\RezeroAgent.exe"; Description: "Launch RezeroAgent"; Flags: nowait postinstall skipifsilent

[Code]
function KillImage(const ImageName: string): Boolean;
var
  ResultCode: Integer;
  Params: string;
begin
  Params := '/C taskkill /F /T /IM "' + ImageName + '" >nul 2>nul';
  Result := Exec(ExpandConstant('{cmd}'), Params, '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure ForceStopRezeroAgent;
var
  I: Integer;
begin
  { Retry to handle child processes and delayed exits. }
  for I := 1 to 8 do
  begin
    KillImage('RezeroAgent.exe');
    Sleep(350);
  end;
end;

function InitializeSetup(): Boolean;
begin
  ForceStopRezeroAgent;
  Result := True;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
  begin
    { Final hard stop right before file copy. }
    ForceStopRezeroAgent;
  end;
end;
