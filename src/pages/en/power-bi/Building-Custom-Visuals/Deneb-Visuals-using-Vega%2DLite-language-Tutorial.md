![Stage of the Wiki](https://img.shields.io/badge/Progress-Published-green)

# Introduction
Deneb is a custom visual for Microsoft Power BI, which allows developers to use the declarative JSON syntax of the Vega-Lite (or Vega) languages to create their own data visualisations. The need for creating this tutorial on how to create and modify custom visuals stems from having limited Power BI sotre visuals to select from. This tutorial is an introduction to creating Power BI visuals using the Deneb package and Vega-Lite language. 

# Overview
This tutorial steps users through installation, introduces them to basic Deneb concepts and key Vega-Lite functions as well as Lighthouse practices when creating or adjusting custom visuals. 
[[_TOC_]]

# Installation

_Expected time: 2mins_

## Beta
1. Download the package required for creating and importing Power BI friendly graphs using the Vega-Lite or Vega coding languages by clicking [here](https://github.com/deneb-viz/deneb/releases/download/0.6.0.150/deneb.0.6.0.150.pbiviz).
2. Import this package by clicking the three dots, followed by the "Import a visual from a file" and selecting this file on your computer

![image.png](/.attachments/image-0c66a1a1-f8a7-408c-bf6b-3ae1c71ec462.png)

## GA 
1. Click the three dots and select "Get more visuals"

![image.png](/.attachments/image-491fbfde-9be8-4240-9792-a72734d2076e.png)

2. Search for deneb and select the tile that looks like the below

![image.png](/.attachments/image-85f0b9ea-03a7-484d-a6fe-0917ef1045f2.png)

_**Note:** You can pin it for future use by right clicking the Deneb icon and selecting "Pin to visualiszations pane". If you do this then occasionally you'll need to re-import Deneb into Power BI either because Power BI will not properly recognise the icon and replace it with a "?" or if there is an update to Deneb that you want to use._ 

![image.png](/.attachments/image-65ef3ae6-42b5-43ea-870f-693f7bfc1bbe.png)

# Fundamentals

_Expected time: 5mins_

## Deneb Features

Making visuals using Deneb is similar to the approaches used for creating R and Python visuals in Power BI, with the following additional benefits:
- Libraries are packaged with the visual, so no additional dependencies on local libraries or gateways for your end-users when publishing reports _e.g. R requires you to have packages pre-installed and then load the packages each time you need the visuals in Power BI. Alternatively you can install._
- Built for the web, meaning that it's possible to integrate with Power BI's interactivity features, with some additional setup _e.g. Whilst you can filter R visuals using other visuals witin Power BI, you can't filter using the R visuals and there are no tooltips._

- Specifications are rendered inside the Power BI client, rather than being delegated to another location, typically resulting in faster render times for end-users.

## Key Vega-Lite Knowledge

There are three key property's to know when creating a visual using Vega-Lite language:
1. **Data**, describes the visualisation’s data source as part of the specification 
2. **Transformations**, manipulate data prior to (view-level) or inside encoding
3. **Encoding**, encodes the data with visual properties of graphical marks

**Data** ingestion happens at the beginning of your Vega-Lite code. It can either be: 
- Inline data ([values](https://vega.github.io/vega-lite/docs/data.html#inline));

`"data": {
    "values": [
      {"a": "A", "b": 28},
      {"a": "B", "b": 55},
      {"a": "C", "b": 43},
      {"a": "G", "b": 19},
      {"a": "A", "b": 87},
      {"a": "I", "b": 52},
      {"a": "D", "b": 91},
      {"a": "E", "b": 81},
      {"a": "F", "b": 53}
    ]
  }`
- A URL from which to load the data ([url](https://vega.github.io/vega-lite/docs/data.html#url)); 

`"data": {"url": "https://github.com/vega/vega-datasets/blob/next/data/cars.json"}`
- Named data source ([name](https://vega.github.io/vega-lite/docs/data.html#named)) which can be bound at runtime or populated from top-level datasets; or

`"data": {"name": "dataset"}`
- Empty.

View-level **Transformations** are executed next in the order they are specified. Three interesting transformations are:
1. Aggregate, 
- Summarises a table as one record for each group;
2. Join Aggregate; 
- Preserves the original table structure and augments records with aggregate values rather than summarising the data in one record for each group; and 
3. Calculate.
- Extends data objects with new fields (columns) according to Vega’s [expression](https://vega.github.io/vega/docs/expressions/) language for writing basic formulas.

Open up the online [Vega-Lite editor](https://vega.github.io/editor/), paste the following code in `{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "description": "A simple bar chart with embedded data that uses a filter and calculate.",
  "data": {
    "values": [
      {"a": "A", "b": 28},
      {"a": "B", "b": 55},
      {"a": "C", "b": 43},
      {"a": "G", "b": 19},
      {"a": "A", "b": 87},
      {"a": "I", "b": 52},
      {"a": "D", "b": 91},
      {"a": "E", "b": 81},
      {"a": "F", "b": 53}
    ]
  },
  "transform": [
    {transform}
  ],
  "mark": "bar",
  "encoding": {
    "y": {"field": "b2", "type": "quantitative"},
    "x": {"field": "a", "type": "ordinal"}
  }
}` and replace the transform braces `{transform}` with the below examples:
1. Aggregate `{"aggregate": [{
       "op": "average",
       "field": "b",
       "as": "b2"
       }],
       "groupby": ["a"]
    }`
2. Join Aggregate `    {"joinaggregate": [{
       "op": "average",
       "field": "b",
       "as": "b2"
       }],
       "groupby": ["a"]
    },
`
3. Calculate `{"calculate": "2*datum.b", "as": "b2"}`

The **Encoding** property of a single view specification represents the mapping between encoding channels (such as x, y, or color) and data fields, constant visual values, or constant data values (datum).

The keys in the encoding object are [encoding channels](https://vega.github.io/vega-lite/docs/encoding.html#channels). Vega-Lite supports a number of encoding channel groups all of which must be one of the following encoding channels:
1. Field, describes the data field encoded by the channel
2. Value, describes an encoded constant visual value
3. Datum, describes a constant data value encoded via a scale

_All Vega-Lite property documentation can be found [here](https://vega.github.io/vega-lite/docs/)._

# Vega-Lite Templaes

_Expected time: 13mins_

## Creating Template
The Vega-Lite website is a great source of [examples](https://vega.github.io/vega-lite/examples/) containing template code. It is a great place to start if you're trying to find a visualisation that hasn't been made by Microsoft yet. 

For this tutorial we'll use the dataset downloadable [here](https://spo-global.kpmg.com/sites/NZ-Lighthouse/_layouts/15/download.aspx?UniqueId=30a8cd93368b445190070aa14e8b08b4&e=a6Mpar)

1. Read in the data you've just downloaded and click Deneb visual you imported earlier.
![image.png](/.attachments/image-2afed6d7-ae3b-41a1-9f07-bf2a90d786ca.png)

2. Click the object and select the fields you are going to need (percentage, question and type).
![image.png](/.attachments/image-4d6fd1f4-730e-4606-87e7-35bdde9835b6.png)

3. Edit the visual, select blank template and click create.
![image.png](/.attachments/image-b5bb6aec-00e0-4170-a5eb-51d47902bf0b.png)

4. Copy [template code](https://vega.github.io/vega-lite/examples/bar_diverging_stack_transform.html) (from "transform" to the penultimate "}") into the editor over the top of `"mark": null`.

5. Push play and strech visualisation out for your results.
![image.png](/.attachments/image-6d5ee48e-7fbf-46cd-bae5-21079072c576.png)

You'll notice this visualisation is completely wrong and when you read through the source code there isn't really any indication of why that is. 

6. Transform the data in Power BI Add the below column in the last Power BI transformation step. This can be done either using the conditional picture below or pasting `,#"Added Conditional Column" = Table.AddColumn(#"Changed Type", "answer order", each if [type] = "Strongly disagree" then 1 else if [type] = "Disagree" then 2 else if [type] = "Neither agree nor disagree" then 3 else if [type] = "Agree" then 4 else if [type] = "Strongly agree" then 5 else null)
in #"Added Conditional Column"` over the top of `in #"Changed Type"` in the Advanced Editor.
![image.png](/.attachments/image-0ed52a4d-cdc3-4195-9725-b2179a137bbe.png)

7. Close & Apply the query and then unselect all fields used for this visual.
![image.png](/.attachments/image-4f1f6b75-ed29-47f4-af58-14687b0154f9.png)

Do the below in order if you have not been able to get the data working:
- Click Transform data;
- Right click and make a new blank query 
- Right click the query and paste the below code over the top of everything
- Click done and go to the "Removed Other Columns" step in Power querry
- Click the double down arrow button and click then Close & Apply

`let Source = SharePoint.Files("https://spo-global.kpmg.com/sites/NZ-Lighthouse", [ApiVersion = 15]), #"Filtered Rows" = Table.SelectRows(Source, each ([Name] = "LikertDataset.json")), #"Removed Other Columns" = Table.SelectColumns(#"Filtered Rows",{"Content"}),  #"Added Conditional Column" = Table.AddColumn(#"Changed Type", "answer order", each if [type] = "Strongly disagree" then 1 else if [type] = "Disagree" then 2 else if [type] = "Neither agree nor disagree" then 3 else if [type] = "Agree" then 4 else if [type] = "Strongly agree" then 5 else null) in #"Added Conditional Column"`

8. Select the answer order field first and then the same fields again. You'll see that your visual has rendered correctly!
![image.png](/.attachments/image-425651c7-22b4-4147-b2ea-df5f2ba71a1e.png)

Do the below in order if you have not been able to get the visual working:
- Step 1 (only the deneb) 
- Step 2
- Step 3
- Step 4
- Step 8

9. Now edit the visual again and click the "Generate JSON Template" button ![image.png](/.attachments/image-40bd2e1c-5774-416e-9def-dabd0b3751ed.png) to bring up the "Create JSON Template" window.
![image.png](/.attachments/image-03d89035-dbe4-4293-a0e7-0216eea3f474.png)

10. We'll go into more details about Lighthouse documentation later in this tutorial, but for now fill out "Template Information" followed by "Dataset (Columns and Measures)" and copy the "Generated Template" JSON to a new file named after your template.

After all this, you'll be able to import a template (your new visual template) rather than step through all these steps. For those of you who haven't been able to get to this step please download [this](https://spo-global.kpmg.com/sites/NZ-Lighthouse/_layouts/15/download.aspx?UniqueId=30a8cd93368b445190070aa14e8b08b4&e=ehQg86) and import.

## Extending Template

You'll notice this situation is a special case by looking at the dataset and code makeup. The dataset driving our graph is made of static totals (value and percentage) which are recalcualted at the codes head. We are likely to get data at an individual level which will include more than just the four columns we started with (question, type, value and percentage) and will need to calculate totals. 

Typically, Power BI users hover their mouse over bars for key information (Tooltips). They also have the ability to filter other visuals by clikcing bars and seeing them highlight.

This means we'll need to implement ideas touched on in the [Fundamentals](https://kpmg-nz.visualstudio.com/Lighthouse/_wiki/wikis/Lighthouse.wiki/229/Deneb-Visuals-using-Vega-Lite-language-Tutorial?anchor=fundamentals) section to the code for a more scalable visual:
1. Apply Transformations;
2. Add Tooltips; and
3. Filter selections (less relevant to this data).

Re-implement Steps 2, 3 and 4 of the Create Template section whilst selecting **value** this time instead of percentage.

**Transformations** 

At the top of the code we need to insert a couple steps to calculate percentage from value. **Question 1:** What calculations do we need to use in this situation? 

![image.png](/.attachments/image-53bcedad-6020-481a-af89-0832bc1bf4ae.png)

Again, this situation is special and we would probably use a distinct count on IDs to remove duplicate responses before calculating their totals in row level data. The code for that, assuming the ID is value, would be `
    {
      "aggregate": [
        {
          "op": "distinct",
          "field": "value",
          "as": "value"
        }
      ],
      "groupby": [
        "question",
        "type"
      ]
    },{
      "joinaggregate": [
        {
          "op": "count",
          "field": "value",
          "as": "Total"
        }
      ],
      "groupby": ["question"]
    },
    {
      "calculate": "datum.value/datum.Total * 100",
      "as": "Percentage"
    }`. This does make everything even because of the data struture (same counts per question), but notice how we use aggregate at the beginning to remove duplicates and change the data struture.

![image.png](/.attachments/image-d2c00d9e-eecd-4e05-9462-dfcd0e62e610.png)

**Tooltips** 

Within the code we need to specify variables to display should a user want more information. **Question 2:** Where in the code do we put our tooltip code? **Question 3:** What should the code display? You'll see after implementing the tooltip code along with the transformations above that the percentages are unrounded. 

![image.png](/.attachments/image-c21d6dcb-7cec-40e8-9bb2-9ae92b4bc2b3.png)

You can format within the tooltip e.g place `,
        "format": ".2f"` after the `"title": "Percentage"` line of code. The better way to do this is by replacing replacing the calculation step `{
      "calculate": "datum.value/datum.Total * 100",
      "as": "Percentage"
    }` with `{
      "calculate": "format(datum.value/datum.Total * 100, '.2f')",
      "as": "Percentage"
    }`. Why? Both will yield the below tooltip, but when there are no decimals the second will no show the ".00" after.

![image.png](/.attachments/image-c734ead5-fc0c-4699-9a02-073cbc2a5618.png)

**Selections** 

This part isn't really relevant here as we're not filtering anything, but it looks cool when implemented! **Question 4:** What do we need to do to make selections? 

_**Note:** Highlighting the selections takes further work so we'll look at this in the [Power BI Visualisations Store Dashboard](https://app.powerbi.com/groups/67156b69-0a52-4040-997e-d7b574f6d15a/reports/ac51c9e9-970b-4c0e-80b8-e6c59cebcb7f?ctid=deff24bb-2089-4400-8c8e-f71e680378b2&pbi_source=linkShare)_ 

# Lighthouse

_Expected time: 5mins_

No real client data can be used in creating a custom visual for the Power BI Visualisations Store. Only Lighthouse can access/update visuals displayed in the [Power BI Visualisations Store Dashboard](https://app.powerbi.com/groups/67156b69-0a52-4040-997e-d7b574f6d15a/reports/ac51c9e9-970b-4c0e-80b8-e6c59cebcb7f?ctid=deff24bb-2089-4400-8c8e-f71e680378b2&pbi_source=linkShare) 

![image.png](/.attachments/image-96c851ef-215b-41bd-a90c-1dddbbcfce5d.png)

or download/update templates from the [Power BI Visualisations Store SharePoint](https://spo-global.kpmg.com/:f:/r/sites/NZ-Lighthouse/Shared%20Documents/Power%20BI%20Visualisations%20DB?csf=1&web=1&e=qmaJgf). 

![image.png](/.attachments/image-8f1a6c21-0ed0-4cc7-96e2-8fa8dc3f12b8.png)

Datasets backing up the visualisations need to be placed within the "Datasets" folder on SharePoint (see image above).

## Power BI Marketplace

Each tab within the [dashboard](https://app.powerbi.com/groups/67156b69-0a52-4040-997e-d7b574f6d15a/reports/ac51c9e9-970b-4c0e-80b8-e6c59cebcb7f?ctid=deff24bb-2089-4400-8c8e-f71e680378b2&pbi_source=linkShare) has one custom visual and is named according to it's SharePoint file name. A short description of the visuals functionality and data model requirements are to be in the top left of the dashbaord with the visual directly underneath. If the visual interacts with other tabs or charts these would need to be indicated within this text box and exist within this dashboard.

## SharePoint Documentation

Every Json file within this [SharePoint](https://spo-global.kpmg.com/:f:/r/sites/NZ-Lighthouse/Shared%20Documents/Power%20BI%20Visualisations%20DB?csf=1&web=1&e=qmaJgf) are templated versions of the dashboard tabs. When importing these templates they have descriptions of the visual and its fields. They have been placed in SharePoint for recovery i.e. incase a tab is deleted or a visual is updated wrong. Again, datasets need to be placed within the "Datasets" folder here as well.

# Future Research

- Exploring other Vega-Lite coding property's, especially the config as this only looks at the main settings. 
- Looking at Vega coding implementation as the [sunburst](https://vega.github.io/vega/examples/sunburst/) doesn't have a Vega-Lite template ([dataset](https://raw.githubusercontent.com/vega/vega-datasets/next/data/flare.json)). Could do this for [New Zeland Vaccinations data](https://www.health.govt.nz/system/files/documents/pages/covid_vaccinations_09_11_2021.xlsx) displayed [here](https://covid19map.co.nz/vaccination-rates/suburb/) or using our [Lighthouse management structure](https://spo-global.kpmg.com/:x:/r/sites/NZ-Lighthouse/Shared%20Documents/Power%20BI%20Visualisations%20DB/Tutorial/Lighthouse.xlsx?d=we2874796cece47a79d8f4eb69b93dd9b&csf=1&web=1&e=AXYLt1) taken from MS Teams.

# References

## Websites

[Deneb Tutorial](https://deneb-viz.github.io/)

[Vega-Lite Documentation](https://vega.github.io/vega-lite/docs/)

[Vega-Lite Templates](https://vega.github.io/vega-lite/examples/)

[Tutorial Code](https://vega.github.io/editor/#/examples/vega/sunburst)

[Tutorial Dataset](https://github.com/vega/vega/blob/master/docs/data/flare.json)

## Questions

**Answer 1:** `{
      "joinaggregate": [
        {
          "op": "sum",
          "field": "value",
          "as": "Total"
        }
      ],
      "groupby": ["question"]
    },
    {
      "calculate": "datum.value/datum.Total * 100",
      "as": "Percentage"
    },`

![image.png](/.attachments/image-70f451c1-6407-4cbb-b8b3-8c6f2078c0db.png)

**Answer 2:** Within and at the end of encoding braces i.e `"encoding": { ... HERE }`

**Answer 3:** `,"tooltip": [
      {
        "field": "question",
        "type": "nominal",
        "title": "Question"
      },
      {
        "field": "Percentage",
        "type": "quantitative",
        "title": "Percentage"
      },
      {
        "field": "value",
        "type": "quantitative",
        "title": "Count"
      },
      {
        "field": "Total",
        "type": "quantitative",
        "title": "Total"
      }
    ]`

![image.png](/.attachments/image-c21d6dcb-7cec-40e8-9bb2-9ae92b4bc2b3.png)

**Answer 4:** We need to layer in selections. Add `"layer": [
    {
      "mark": "bar",
      "encoding": {
        "opacity": {
          "condition": {
            "test": {
              "field": "__selected__",
              "equal": "off"
            },
            "value": 0.5
          }
        }
      }
    }
  ],` in front of the encoding for selections.

