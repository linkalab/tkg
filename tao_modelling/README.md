# README #

This repository contains data and code necessary to build TAO ontology from scratch and to compute its ontology metrics.

### Setup and run the notebooks ###

The code has been tested with Python 3.8.10 but it should work with any 3.x version of Python.

You need [Jupyter](https://jupyter.org/) to run the code. First create and activate a virtual env and then install all necessary depenency modules.

```
python3 -m venv myenv
source myenv/bin/activate
pip install wheel
pip install -r requirements.txt
```

Now you can launch Jupyter Notebook server:
```
jupyter-notebook
```
and use the browser to load and execute one of the following notebook files:
* TAO_ontology_builder.ipynb
* TAO_class_exporter.ipynb
* load_ontometrics_evaluation.ipynb

#### TAO_ontology_builder

This is the first notebook to run. It creates the TAO ontology using Owlready2.
After the notbook is successfully compelted a new *tao.rdf* file would be created under output_ontology directory.

#### TAO_class_exporter

This notebook uses the SQLite database created by the first notebook to produce csv files containing all the classes and labels from the TAO ontology.
The resulting files are saved under class_mapping directory.

#### load_ontometrics_evaluation
This notebook is under the ontology_metrics directory. It creates latex files containig ontology metrics produced using [Ontometrics web application](https://ontometrics.informatik.uni-rostock.de/ontologymetrics/index.jsp).


### License ###

This project is licensed under the Apache License 2.0.

See [LICENSE](LICENSE.txt) for more information.

This is a project created by [Linkalab](http://www.linkalab.it) in collaboration with the [Department of Mathematics and Computer science of the University of Cagliari](https://unica.it/unica/en/dip_matinfo.page) and [Knowledge Media Institute of The Open University](https://kmi.open.ac.uk/).