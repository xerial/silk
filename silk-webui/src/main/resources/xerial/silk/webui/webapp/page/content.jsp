  <jsp:include page="/page/header.jsp">
    <jsp:param name="title" value="Silk Weaver"/>
  </jsp:include>

    <div class="container-fluid">
      <div class="row-fluid">
        <div class="span2">
         <jsp:include page="/page/sidebar.jsp"/>

        </div><!--/span-->
        <div class="span9">
          <%= request.getAttribute("content") %>
        </div><!--/span-->
      </div><!--/row-->

<jsp:include page="/page/footer.jsp"/>
