# Automation ArcGIS Vector Tile Layer (VTL)
A scripted process for automatic generation of pre-styled [Vector Tile Layer for ArcGIS Online/ArcGIS Enterprise apps](https://pro.arcgis.com/en/pro-app/3.1/help/data/services/use-vector-tiled-layers.htm) 

## Intro
Working with Web GIS layers in ArcGIS Online or ArcGIS Enterprise can be very easy, as long as it's done interactively via a web browser or an Esri client application such as ArcGIS Pro.

Once regular updates to layers are needed, the story changes quite a bit and in this post I want to give an example for using `arcpy` and the `ArcGIS Python API` to script regular updates to a pre-styled Vector Tile Layer.

Imagine you have data which change on a regular basis and that need to be provided through ArcGIS Online for consumption by external customers via a web app. ArcGIS Online cannot make a connection to the data your organisation's network, so data need to be uploaded. You can do that interactively, but as soon as you'll need to make updates more frequently, you'll want some form of automation for it. That's what the `ArcGIS Python API` is perfect for.

## Requirements

[Here's my example](https://topnetvisdemo.netlify.app):

    > A country-wide dataset (ca. 2 million line geometries) that needs to be updated 4 times daily, using new data. The geometries are static in our case, it's the attributes which are attached to each of the line segments which change and which are used for symbolising the lines in different colours/hues, indicating the level of an attribute.

* A public-facing application will be used to display the data in a zoomable map. Good performance needed at all zoom levels
* The example application above ultimately also needs to allow a user to click on a line segment to create a current chart of data, but that process is separate from the Vector Tile Layer updates which I describe in this post (as it uses a live layer connection via ArcGIS Enterprise).
* 4 times daily updates (attributes only)
* ca. 2 Mio records (12 attributes)
