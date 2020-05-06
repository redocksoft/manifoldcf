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
  if (editconnection.replacementsfileupload.value === "")
  {
    alert("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('reDockRedactor.ChooseAReplacementsFile'))");
    editconnection.replacementsfileupload.focus();
  }
  else if(editconnection.replacementspath.value === "" || confirm("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('reDockRedactor.ConfirmReplacementsPathOverwrite'))"))
  {
    editconnection.configop.value = "Add";
    postForm();
  }
}
</script>
