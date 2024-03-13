# JDBC.NET
[![Nuget](https://img.shields.io/nuget/v/JDBC.NET.Data)](https://www.nuget.org/packages/JDBC.NET.Data/)  
It is a wrapper that allows you to use JDBC drivers in ADO.NET

## Getting Started
### 1. Install NuGet package
Install the latest version of the **JDBC.NET.Data** package from NuGet.

### 2. Add J2NET Runtime package reference
Paste the following XML into your Project(*.csproj / .vbproj / .fsproj*) file.

```xml
<PropertyGroup>
    <RuntimeVersion>1.3.2</RuntimeVersion>
    <OSPlatform Condition="'$([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform($([System.Runtime.InteropServices.OSPlatform]::OSX)))' == 'true'">OSX</OSPlatform>
    <OSPlatform Condition="'$([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform($([System.Runtime.InteropServices.OSPlatform]::Linux)))' == 'true'">Linux</OSPlatform>
    <OSPlatform Condition="'$([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform($([System.Runtime.InteropServices.OSPlatform]::Windows)))' == 'true'">Windows</OSPlatform>
    <OSArchitecture>$([System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture)</OSArchitecture>
</PropertyGroup>

<ItemGroup>
    <PackageReference Condition=" '$(OSPlatform)' == 'OSX' And '$(OSArchitecture)' == 'X64' " Include="J2NET.Runtime.Mac" Version="$(RuntimeVersion)" />
    <PackageReference Condition=" '$(OSPlatform)' == 'Linux' And '$(OSArchitecture)' == 'X64' " Include="J2NET.Runtime.Linux" Version="$(RuntimeVersion)" />
    <PackageReference Condition=" '$(OSPlatform)' == 'Windows' And '$(OSArchitecture)' == 'X64' " Include="J2NET.Runtime.Win64" Version="$(RuntimeVersion)" />
    <PackageReference Condition=" '$(OSPlatform)' == 'Windows' And '$(OSArchitecture)' == 'X86' " Include="J2NET.Runtime.Win32" Version="$(RuntimeVersion)" />
</ItemGroup>
```

### 3. Connect to Database!
```csharp
var builder = new JdbcConnectionStringBuilder
{
    DriverPath = "mysql-connector-java-8.0.21.jar",
    DriverClass = "com.mysql.cj.jdbc.Driver",
    JdbcUrl = "jdbc:mysql://127.0.0.1/sakila?user=root&password=12345"
};

using var connection = new JdbcConnection(builder);
connection.Open();
```
