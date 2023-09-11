[[_TOC_]]

##Introduction
Power BI allows developers to add custom visuals into Power BI to use in dashboards and reports. There are existing visuals developed by the community which are available in the marketplace, or you can add them from a file.

##Building new visuals
Power BI visuals are written in Typescript. Typescript is an open source language from Microsoft that is built on top of Javascript. Using Javascript means that there are many existing open source libraries available, such as D3 for rapidly creating graphic effects.

##Prerequisites
You will need:
- Power BI premium
- [node.js](https://nodejs.org/en/)
- pbiviz
- Visual Studio Code (any IDE of your choice)

##Setting Up

###Power BI Visuals Tools
Run in cmd:
<pre>npm i powerbi-visuals-tools</pre>

###Creating and Installing a Certificate
The certificate allows you to make a connection between your machine and Power BI
<pre>pbiviz --install-cert</pre>

1. This command should return a passphrase and prompt the Certificate Import Wizard. **Copy this passphrase**

![image.png](/.attachments/image-75d480da-32a1-49b6-ba37-9499ceec4dca.png)

2. Select **Current User**
3. In the File to Import window, select **Next**
4. **Paste** the passphrase in the password field
5. In the Certificate store, select **Place all certificates in the following store option**, and select **Browse**
6. In the Select Certificate Store window, select **Trusted Root Certification Authorities** and then select **OK**
7. Select **Next** in the Certificate Store window.


###Power BI Service

To develop a Power BI visual, you'll need to enable custom visual debugging in Power BI service

1. Log in to [app.powerbi.com](https://app.powerbi.com/)
2. Go to **Settings > Settings**

![image.png](/.attachments/image-2694ef3c-abc7-4b9c-bd11-f7ee3bba93a7.png)
3. Go to **General > Developer**, **enable developer mode**, and select **apply**

![image.png](/.attachments/image-c749265d-f7b3-4472-88f2-c98e607fc12f.png)

##Starting your project

Create you project:

<pre>pbiviz new [Your Project Name] </pre>

Get your project running

<pre>pbiviz start</pre>

![image.png](/.attachments/image-8ec1858a-2698-4f96-9e0f-e2d0e1c6e4d7.png)

Check https://localhost:8080/webpack-dev-server/ to confirm that your project is running

##Viewing your Visual

1. Log in to [app.powerbi.com](https://app.powerbi.com/)
2. Create a new report
3. Add data to create a report
4. When the report is created, select **Edit**
5. Under **Visualisations**, select the **Developer Visual** and select **Auto toggle reload**

##Creating your package

Run the code below in your terminal, to package your visual and create a pbiviz file.

<pre>pbiviz package</pre>

You can find the pbiviz file in the **dist** folder. You can then import this file in Power BI desktop to use






