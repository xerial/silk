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



