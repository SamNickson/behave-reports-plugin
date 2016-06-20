/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Orangesoft
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the 
 * Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.orangesoft.jenkins;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;

import org.kohsuke.stapler.StaplerRequest;
import org.kohsuke.stapler.StaplerResponse;

import hudson.FilePath;
import hudson.model.Action;
import hudson.model.DirectoryBrowserSupport;
import org.orangesoft.behave.ReportBuilder;

public abstract class CucumberReportBaseAction implements Action {

    protected static final String BASE_URL = "cucumber-html-reports";

    private static final String DEFAULT_PAGE = ReportBuilder.HOME_PAGE;

    @Override
    public String getUrlName() {
        return BASE_URL;
    }

    @Override
    public String getDisplayName() {
        return Messages.SidePanel_DisplayName();
    }

    @Override
    public String getIconFileName() {
        return "/plugin/cucumber-reports/cuke.png";
    }

    public void doDynamic(StaplerRequest req, StaplerResponse rsp) throws IOException, ServletException {
        // since Jenkins blocks JavaScript as described at
        // https://wiki.jenkins-ci.org/display/JENKINS/Configuring+Content+Security+Policy and fact that plugin uses JS
        // to display charts, following must be applied
        System.setProperty("hudson.model.DirectoryBrowserSupport.CSP",
                "sandbox; script-src 'self' 'unsafe-inline'; default-src 'self'; img-src 'self'; style-src 'self';");

        DirectoryBrowserSupport dbs = new DirectoryBrowserSupport(this, new FilePath(dir()), getTitle(), getUrlName(),
                false);

        dbs.setIndexFileName(DEFAULT_PAGE);
        dbs.generateResponse(req, rsp, this);
    }

    protected abstract String getTitle();

    protected abstract File dir();
}
