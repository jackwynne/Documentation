---
title: Add Windows Store
description: How to add the Windows Store to a computer where it has been removed
lang: en
---

Installing the Windows Store allows you to install apps from the Store (e.g. Power Bi, Terminal, Powershell, Python) and they are automatically updated.
You may need to navigate to the folder in Windows Explorer before running the command.

## How

```powershell
> Add-AppxPackage -Path L:\Software\Windows\Microsoft.WindowsStore_12107.1001.15.0_neutral___8wekyb3d8bbwe.AppxBundle
```

### Add Winget

Once the Microsoft Store is installed, hit Windows Key + R and paste in the text below which will take you to the page where you can install the package manager.

```powershell
ms-windows-store://pdp/?productid=9nblggh4nns1
```
