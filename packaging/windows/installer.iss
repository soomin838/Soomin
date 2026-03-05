[Setup]
AppId={{8C8A0F3A-DC89-4D6D-B9E5-9D8CD5A9D981}
AppName=RezeroAgent
AppVersion=1.0.0
DefaultDirName={pf}\RezeroAgent
UsePreviousAppDir=no
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
SetupIconFile=..\..\ui\assets\rezero_icon.ico
UninstallDisplayIcon={app}\RezeroAgent.exe

[Files]
Source: "..\..\dist\RezeroAgent.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\dist\version.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\ui\assets\rezero_icon.ico"; DestDir: "{app}"; Flags: ignoreversion
; Keep a desktop executable in sync for users who launch directly from Desktop.
Source: "..\..\dist\RezeroAgent.exe"; DestDir: "{autodesktop}"; DestName: "RezeroAgent.exe"; Flags: ignoreversion

[InstallDelete]
Type: files; Name: "{autodesktop}\RezeroAgent.lnk"
Type: files; Name: "{group}\RezeroAgent.lnk"
; Remove stale standalone desktop executables so users always run the installed binary.
Type: files; Name: "{autodesktop}\RezeroAgent.exe"

[Icons]
Name: "{group}\RezeroAgent"; Filename: "{app}\RezeroAgent.exe"; IconFilename: "{app}\rezero_icon.ico"
Name: "{group}\Uninstall RezeroAgent"; Filename: "{uninstallexe}"
; Always create a desktop shortcut in active install context.
Name: "{autodesktop}\RezeroAgent"; Filename: "{app}\RezeroAgent.exe"; IconFilename: "{app}\rezero_icon.ico"

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
