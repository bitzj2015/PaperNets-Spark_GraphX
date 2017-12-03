# PaperNets-Spark_GraphX
This project constructs an academic paper graph using Spark-GraphX framework. The dataset derives from Microsoft Academic Graph (MAG) and AMiner, which can be downloaded from https://www.openacademic.ai/oag/. Besides, some analysis tasks have been done based on this graph.
Every vertexes of this graph contains three properties of one paper: id, issue and title; every edges of this graph connects two papers which have reference relations and contains one property: year.
Pagerank Algorithm is utilized to find papers with significant influence and connectedcomponents are found to analyse academic groups.
