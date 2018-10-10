# fastest-bus-analysis-in-the-west

## Code to develop data for Speed and Reliability Analysis

This program pulls data from different sources like Swiftly and ridership and other sources for bus and light rail routes, to better quantify and visualize ridership and speed information.

Many of the resources and data are internal to the VTA.  Access is via a config file not found in the repo.

[Tableau visualizations](https://public.tableau.com/profile/vivek7797#!/vizhome/stopsandspeedanalyses/Story1)

[More visualizations](https://public.tableau.com/profile/jason.kim2675#!/vizhome/shared/WW7CX4NR2)

[Presentation](https://docs.google.com/presentation/d/1zFiVGcD00LJvth6HnL4r9Qq3CMS0cn0KEvhP0MtWZcw/edit#slide=id.g42f5f7eb23_0_45)

## How to Use this

1. Get a ```config``` file from Vivek/Jason (which has passwords!) as well as Swiftly data and Ridership Data.
1. Set up a Jupyter Notebook environment in Python 3, install packages for ```make-all-faster-buses-good.ipynb```
  (requirements file incoming!)
1. Open and configure parameters inside the notebook ```make-all-faster-buses-good.ipynb```, produces results intermediate files
1. Open and run ```fast_csv_and_geospatial_output.ipynb```, this adds additional metrics and produces data attached stops and routes shapes.

## Outputs

[Outputs](https://drive.google.com/open?id=1VPZKqXr_yC8KNpr9z5zdjP1Ay82svuly)

This data are estimates generated through algorithms and summarization can be interpreted many different ways.  This data can also have errors, please be careful when interpreting.
