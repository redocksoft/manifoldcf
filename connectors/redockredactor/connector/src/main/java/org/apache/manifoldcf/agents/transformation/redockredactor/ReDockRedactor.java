/**
 * Copyright reDock Inc. 2020, All Rights Reserved
 */

package org.apache.manifoldcf.agents.transformation.redockredactor;

import com.redock.redactor.lib.Redactor;
import com.redock.redactor.lib.RedactorInvalidReplacements;
import com.redock.redactor.lib.RedactorUnsupportedFileType;
import org.apache.commons.io.IOUtils;
import org.apache.manifoldcf.agents.interfaces.IOutputAddActivity;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.agents.system.ManifoldCF;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.crawler.system.Logging;
import org.apache.manifoldcf.ui.i18n.Messages;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This connector uses the Redactor from reDock to obfuscate documents. The obfuscation is based off replacements
 * that must be provided in the connector`s configuration. The replacements are a pair of strings (usually in a TSV file,
 * one per line) where the first value is the "target" (either a Regex OR a word to be perfectly matched) and the
 * second value is a "replacement" value that will be inserted in place of the target value in the documents.
 */
public class ReDockRedactor extends org.apache.manifoldcf.agents.transformation.BaseTransformationConnector {
    public static final String DEFAULT_BUNDLE_NAME="org.apache.manifoldcf.agents.transformation.redockredactor.common";
    public static final String DEFAULT_PATH_NAME="org.apache.manifoldcf.agents.transformation.redockredactor";

    public static final String _rcsid = "@(#)$Id$";

    protected static final String ACTIVITY_PROCESS = "process";
    protected static final String[] activitiesList = new String[]{ACTIVITY_PROCESS};

    /**
     * Configuration tab for this connector
     */
    private final static String CONFIG_TAB = "reDockRedactor.Configs";

    /**
     * Forward to the javascript to check the configuration parameters
     */
    private static final String CONFIG_HEADER = "editConfiguration.js";

    /**
     * Forward to the HTML template to edit the configuration parameters
     */
    private static final String EDIT_CONFIG = "editConfiguration_Config.html";

    /**
     * Forward to the HTML template to view the configuration parameters
     */
    private static final String VIEW_CONFIG = "viewConfiguration.html";

    /**
     * Use to easily read files from the file-resources folder within ManifoldCF's installation folder
     */
    protected final File fileDirectory = ManifoldCF.getFileProperty(ManifoldCF.fileResourcesProperty);

    /**
     * reDock Redactor
     */
    protected Redactor redactor = new Redactor();

    @Override
    public void install(IThreadContext threadContext) throws ManifoldCFException {
        super.install(threadContext);
        new ReplacementsManager(threadContext).initialize();
    }

    @Override
    public void deinstall(IThreadContext threadContext) throws ManifoldCFException {
        super.deinstall(threadContext);
        new ReplacementsManager(threadContext).destroy();
    }

    @Override
    public String check() throws ManifoldCFException {
        try {
            Map<String, String> replacements = retrieveReplacements(getConfiguration(), currentContext);
            if (replacements.size() == 0) {
                return "No replacements found.";
            }
            redactor.validateReplacements(replacements);
        } catch (RedactorInvalidReplacements e) {
            return "Invalid replacements" + e.getMessage();
        } catch (Exception e) {
            return "Error checking connector status: " + e.getMessage();
        }

        return super.check();
    }

    /**
     * Return a list of activities that this connector generates.
     * The connector does NOT need to be connected before this method is called.
     *
     * @return the set of activities.
     */
    @Override
    public String[] getActivitiesList() {
        return activitiesList;
    }

    /**
     * Add (or replace) a document in the output data store using the connector.
     * This method presumes that the connector object has been configured, and it is thus able to communicate with the output data store should that be
     * necessary.
     * The OutputSpecification is *not* provided to this method, because the goal is consistency, and if output is done it must be consistent with the
     * output description, since that was what was partly used to determine if output should be taking place.  So it may be necessary for this method to decode
     * an output description string in order to determine what should be done.
     *
     * @param documentURI         is the URI of the document.  The URI is presumed to be the unique identifier which the output data store will use to process
     *                            and serve the document.  This URI is constructed by the repository connector which fetches the document, and is thus universal across all output connectors.
     * @param document            is the document data to be processed (handed to the output data store).
     * @param authorityNameString is the name of the authority responsible for authorizing any access tokens passed in with the repository document.  May be null.
     * @param activities          is the handle to an object that the implementer of a pipeline connector may use to perform operations, such as logging processing activity,
     *                            or sending a modified document to the next stage in the pipeline.
     * @return the document status (accepted or permanently rejected).
     * @throws IOException only if there's a stream error reading the document data.
     */
    @Override
    public int addOrReplaceDocumentWithException(String documentURI, VersionContext pipelineDescription, RepositoryDocument document, String authorityNameString, IOutputAddActivity activities)
            throws ManifoldCFException, ServiceInterruption, IOException {
        long startTime = System.currentTimeMillis();
        String resultCode = "OK";
        String description = null;
        ByteArrayInputStream redacted = null;
        Long length = null;

        try {
            Map<String, String> replacements = retrieveReplacements(getConfiguration(), currentContext);

            if(redactor.supportsFile(document.getFileName(), document.getMimeType())) {
                ByteArrayOutputStream output = null;
                try {
                    output = new ByteArrayOutputStream();
                    redactor.redactStream(document.getBinaryStream(), document.getFileName(), document.getMimeType(), replacements, output, true);
                } finally {
                    IOUtils.closeQuietly(output);
                }

                redacted = new ByteArrayInputStream(output.toByteArray());

                // Use of available() here is only valid because we're dealing with a ByteArrayInputStream.
                document.setBinary(redacted, redacted.available());
            } else {
                Logging.connectors.info("reDockRedactor: Unsupported File Type was not caught by supportsFile() with fileName" + document.getFileName() + " and mimeType " + document.getMimeType());
            }

            int rval = activities.sendDocument(documentURI, document);
            length = document.getBinaryLength();
            resultCode = (rval == DOCUMENTSTATUS_ACCEPTED) ? "ACCEPTED" : "REJECTED";
            return rval;
        } catch (RedactorUnsupportedFileType | RedactorInvalidReplacements e) {
            resultCode = "EXCEPTION";
            description = e.getMessage();
            throw new ManifoldCFException("Provided replacements are invalid: " + e.getMessage(), e, ManifoldCFException.SETUP_ERROR);
        } catch (ServiceInterruption e) {
            resultCode = "SERVICEINTERRUPTION";
            description = e.getMessage();
            throw e;
        } catch (IOException e) {
            resultCode = "IOEXCEPTION";
            description = e.getMessage();
            throw e;
        } finally {
            IOUtils.closeQuietly(redacted);
            activities.recordActivity(startTime, ACTIVITY_PROCESS, length, documentURI,
                    resultCode, description);
        }
    }

    @Override
    public void outputConfigurationHeader(IThreadContext threadContext,
                                          IHTTPOutput out, Locale locale, ConfigParams parameters,
                                          List<String> tabsArray) throws ManifoldCFException, IOException {
        super.outputConfigurationHeader(threadContext, out, locale, parameters, tabsArray);
        tabsArray.add(Messages.getString(ReDockRedactor.class, DEFAULT_BUNDLE_NAME, locale, CONFIG_TAB, null));
        outputResource(CONFIG_HEADER, out, locale, null, null, null, null);
    }

    @Override
    public void outputConfigurationBody(IThreadContext threadContext,
                                        IHTTPOutput out, Locale locale, ConfigParams parameters, String tabName) {
        try {
            super.outputConfigurationBody(threadContext, out, locale, parameters, tabName);

            retrieveReplacements(parameters, threadContext);
            ReDockRedactorConfig config = this.getConfigParameters(parameters);

            outputResource(EDIT_CONFIG, out, locale, config, tabName, null, null);
        } catch (Exception e) {
            Logging.connectors.warn("reDockRedactor: Output configuration body failed", e);
        }
    }

    @Override
    public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters)
            throws ManifoldCFException {

        retrieveReplacements(parameters, threadContext);

        outputResource(VIEW_CONFIG, out, locale,
                getConfigParameters(parameters), null, null, null);
    }

    @Override
    public String processConfigurationPost(IThreadContext threadContext, IPostParameters variableContext, ConfigParams parameters)
            throws ManifoldCFException {
        ReplacementsManager replacementsManager = new ReplacementsManager(threadContext);

        String connectorName = variableContext.getParameter("connname");

        String configOp = variableContext.getParameter("configop");
        if (configOp != null) {
            if (connectorName == null || connectorName.isEmpty()) {
                return "Please set your configuration's name first.";
            }

            if (configOp.equals("Delete")) {
                replacementsManager.deleteGroup(connectorName);
            } else if (configOp.equals("Add")) {
                String appendString = variableContext.getParameter("appendreplacements");
                boolean append = appendString != null && appendString.equals("true");
                if (!append) {
                    replacementsManager.deleteGroup(connectorName);
                }

                byte[] replacementsBytes = variableContext.getBinaryBytes("replacementsfileupload");
                InputStream input = new ByteArrayInputStream(replacementsBytes);

                // This code saves the uploaded file to the file-resources folder. Keeping it around in case we
                // ever want to support that approach in the future.
//                    File replacementFile = new File(fileDirectory.getAbsoluteFile(), connectorName + ".tsv");
//                    OutputStream outStream = new FileOutputStream(replacementFile);
//                    outStream.write(replacementsBytes);

                for (Map.Entry<String, String> entry : redactor.readReplacement(input).entrySet()) {
                    replacementsManager.addReplacement(new ReplacementRow(
                            connectorName,
                            entry.getKey(),
                            entry.getValue()
                    ));
                }
            }
        }

        parameters.setParameter(ReDockRedactorParam.ParameterEnum.CONNECTORNAME.name(), connectorName);
        retrieveReplacements(parameters, threadContext);
        return ReDockRedactorConfig.contextToConfig(variableContext, parameters);
    }

    /**
     * Read the content of a resource, replace the variable ${PARAMNAME} with the
     * value and copy it to the out.
     */
    private static void outputResource(String resName, IHTTPOutput out,
                                       Locale locale, ReDockRedactorParam params,
                                       String tabName, Integer sequenceNumber, Integer currentSequenceNumber) throws ManifoldCFException {
        Map<String, Object> paramMap;
        if (params != null) {
            paramMap = params.buildMap(out);
            if (tabName != null) {
                paramMap.put("TabName", tabName);
            }
            if (currentSequenceNumber != null)
                paramMap.put("SelectedNum", currentSequenceNumber.toString());
        } else {
            paramMap = new HashMap<>();
        }
        if (sequenceNumber != null)
            paramMap.put("SeqNum", sequenceNumber.toString());

        Messages.outputResourceWithVelocity(out, ReDockRedactor.class, DEFAULT_BUNDLE_NAME, DEFAULT_PATH_NAME, locale, resName, paramMap);
    }

    /**
     * Build a Set of reDock parameters. If configParams is null, getConfiguration() is used.
     */
    private ReDockRedactorConfig getConfigParameters(ConfigParams configParams) {
        if (configParams == null) configParams = getConfiguration();
        return new ReDockRedactorConfig(configParams);
    }

    /**
     * Retrieve the replacements using one of the following method (in order of priority):
     * 1- Check if REPLACEMENTSPATH is an absolute path to a file
     * 2- Check if REPLACEMENTSPATH is a relative path to a file inside [ManifoldCF_install_folder]/file-resources
     * 3- Check if there are replacements in the DB
     */
    private Map<String, String> retrieveReplacements(ConfigParams configParams, IThreadContext threadContext) throws ManifoldCFException {
        // Load configuration from parameters
        final ReDockRedactorConfig config = new ReDockRedactorConfig(configParams);
        File replacementsFile = new File(config.getReplacementsPath());
        if (!replacementsFile.exists() && !config.getReplacementsPath().isEmpty()) {
            replacementsFile = new File(fileDirectory, config.getReplacementsPath());
        }

        Map<String, String> replacements = new HashMap<>();
        if (replacementsFile.exists()) {
            replacements = redactor.readReplacements(replacementsFile);
        } else {
            ReplacementsManager replacementsManager = new ReplacementsManager(threadContext);
            ReplacementRow[] rows = replacementsManager.getReplacements(config.getConnectorName());
            for (ReplacementRow row : rows) {
                replacements.put(row.target, row.replacement);
            }
        }

        int replacementsSize = replacements.size();
        configParams.setParameter(ReDockRedactorParam.ParameterEnum.REPLACEMENTSFOUND.name(), Integer.toString(replacementsSize));

        return replacements;
    }
}

