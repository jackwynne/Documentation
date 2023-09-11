[[_TOC_]]

# Overview
## Picture Example
This guide is explains how to unpivot a dataset where there are two value columns for each attribute. 
Before: 
**![image.png](/.attachments/image-3cffd9ed-cd9d-400e-a337-e63aa5342280.png)**
After: 
![image.png](/.attachments/image-272714c8-7dec-4b70-95ee-ea2227c8e4b7.png)

## Full Code
The full code to go from prepared data to unpivoited data is below with the data preparation steps removed:
```
let 
    Source = ...
    ...
    #"Removed Columns1" = ...
    Attributes = List.Distinct(Table.TransformColumns(Table.UnpivotOtherColumns(#"Removed Columns1", {"Proposal Code"}, "Attribute", "Value"), {{"Attribute", each Text.BeforeDelimiter(_, " -"), type text}})[Attribute]),
    #"Added Custom" = Table.AddColumn(#"Removed Columns1", "Custom", each List.Zip( {Attributes, Record.FieldValues(Record.SelectFields( _, List.Transform(Attributes, each _ & " - Adjustment"))), Record.FieldValues(Record.SelectFields( _, List.Transform(Attributes, each _ & " - Justification"))) })),
    #"Removed Other Columns1" = Table.SelectColumns(#"Added Custom",{"Proposal Code", "Custom"}),
    #"Expanded Custom" = Table.ExpandListColumn(#"Removed Other Columns1", "Custom"),
    #"Extracted Values" = Table.TransformColumns(#"Expanded Custom", {"Custom", each Text.Combine(List.Transform(_, Text.From), "{}"), type text}),
    #"Split Column by Delimiter" = Table.SplitColumn(#"Extracted Values", "Custom", Splitter.SplitTextByDelimiter("{}", QuoteStyle.Csv), {"Type", "Adjustment", "Justification"})
in
    #"Split Column by Delimiter"
```

## Video Source
It is based on the guides from Enterprise DNA below.
Basics: https://www.youtube.com/watch?v=U1O5LfMZP0s
Dynamic Construction: https://www.youtube.com/watch?v=aEezNicgHkE

# Step by Step Instructions
## Step One: Data
Set up your data with the column headers in the format "Attribute Name" + "Delimiter" + "Value Name". eg for the attributes "Input" and "Fair Share" and the values "Adjustment" and "Justification" the column names would be "Input-Adjustment", "Input-Justification", "Fair Share-Adjustment" and "Fair Share-Justification". 

## Step Two: Attributes
```
Attributes = List.Distinct(
    Table.TransformColumns(
        Table.UnpivotOtherColumns(#"PreparedData", {"UniqueIdentifier"}, "Attribute", "Value"),
        {{"Attribute", each Text.BeforeDelimiter(_, "Delimiter"), type text}}
    )[Attribute]
)
```
The above code creates a list of attributes from the column names of a prepared dataset. The three variables you need to set are:
- the token for the table once the data has been prepared (#"PreparedData");
- the unique identifier ("UniqueIdentfier") which is the one column which you do not want to unpivot; and
- the delimiter ("Delimiter") which seperates your attribute and value names in the column headers.

The following steps are executed by the code:
1. Unpivot all attribute/value columns;
2. Extract the text before the delimiter in the attribute column which gives just the attribute names; and
3. Get a distinct list.

## Step Three: Zip List 
```
= Table.AddColumn(
    #"PreparedData", "Custom", each List.Zip({
        Attributes, 
        Record.FieldValues(Record.SelectFields( _, List.Transform(Attributes, each _ & "-ValueOne"))), 
        Record.FieldValues(Record.SelectFields( _, List.Transform(Attributes, each _ & "-ValueTwo"))) 
    })
)
```
The above code creates a new column with the zipped lists inside. The three parameters are:
- the token for the table once the data has been prepared (#"PreparedData");
- the column name suffix for the first value including the delimiter ("-ValueOne"); and
- the column name suffix for the seconf value including the delimiter ("-ValueTwo").

The code joins together three lists the attributes, the values for value one and the values for value two.

Once you have removed the other columns (`= Table.SelectColumns(#"Added Custom",{"UniqueIdentifier", "Custom"})`) it will look like this:
![image.png](/.attachments/image-7ecacfce-c36b-4a3e-a819-4db40f48261d.png)

## Step Four: Extract
Click the button with the two arrows beside the column named custom and select "Expand to New Rows".
![image.png](/.attachments/image-4bf909ff-ce31-4b03-a93e-4de607189e16.png)

Click the buttom with the two arrows again and select "Extract Values..."
![image.png](/.attachments/image-09cc0592-52a2-4b88-9fe7-f00ee9e4623c.png)

You will need to set a delimiter which you can then split the column by.