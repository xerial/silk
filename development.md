## Development notes


### Developing silk-webui

 - GWT Compile (required only first time)
    bin/sbt gwt-compile

 - Launch web container
    bin/sbt "~;container:start; container:reload /"

 - Launch GWT code server (super dev mode)
    bin/sbt gwt-superdev

 - Visit http://localhost:9876/ and copy the bookmarklet ("Dev Mode On") appeared in the page to your bookmark bar.

 - Open http://localhost:8080/top.jsp, then click "Dev Mode On".


 If you need to use 192.xx.xx.xx address to access GWT pages, run gwt-superdev mode with -Dgwt.expose=true option

    bin/sbt gwt-superdev -Dgwt.expose=true

 You also need to copy bookmarklet from http://192.xx.xx.xx:9876 since it becomes different when using http://localhost:9876

#### Developping cluster manager web UI

* Install silk command

    $ make install

* List up host names to use in `~/.silk/hosts`, in which each line has a host name. For debugging, add localhost only.

* Start silk cluster

    $ silk cluster start



