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

import hudson.model.AbstractProject;
import hudson.model.ProminentProjectAction;
import hudson.model.Run;

public class CucumberReportProjectAction extends CucumberReportBaseAction implements ProminentProjectAction {

    private final AbstractProject<?, ?> project;

    public CucumberReportProjectAction(AbstractProject<?, ?> project) {
        this.project = project;
    }

    @Override
    protected File dir() {
        Run<?, ?> run = this.project.getLastCompletedBuild();
        if (run != null) {
            File archiveDir = getBuildArchiveDir(run);

            if (archiveDir.exists()) {
                return archiveDir;
            }
        }

        return getProjectArchiveDir();
    }

    private File getProjectArchiveDir() {
        return new File(project.getRootDir(), CucumberReportBaseAction.BASE_URL);
    }

    /** Gets the directory where the HTML report is stored for the given build. */
    private File getBuildArchiveDir(Run<?, ?> run) {
        return new File(run.getRootDir(), CucumberReportBaseAction.BASE_URL);
    }

    @Override
    protected String getTitle() {
        return this.project.getDisplayName();
    }
}