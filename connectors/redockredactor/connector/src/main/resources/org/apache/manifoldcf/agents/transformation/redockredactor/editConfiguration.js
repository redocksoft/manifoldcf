<!--
  Copyright reDock Inc. 2020, All Rights Reserved
-->

<script type="text/javascript">
function RedockRedactorDeleteReplacements()
{
  if(confirm("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('reDockRedactor.ClearReplacementsConfirm'))")) {
    editconnection.configop.value = "Delete";
    postForm();
  }
}

function RedockRedactorAddReplacements()
{
  if (editconnection.replacementsfileupload.value == "")
  {
    alert("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('reDockRedactor.ChooseAReplacementsFile'))");
    editconnection.replacementsfileupload.focus();
  }
  else
  {
    editconnection.configop.value = "Add";
    const test = editconnection.appendreplacements.value;
    postForm();
  }
}
</script>
