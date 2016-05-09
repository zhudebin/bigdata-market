package com.zmyuan.bg.spark.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/** An {@link OutputCommitter} that commits files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CustomFileOutputCommitter extends OutputCommitter {

    public static final Log LOG = LogFactory.getLog(
            "org.apache.hadoop.mapred.CustomFileOutputCommitter");

    /**
     * Temporary directory name
     */
    public static final String TEMP_DIR_NAME =
            MapreduceFileOutputCommitter.PENDING_DIR_NAME;
    public static final String SUCCEEDED_FILE_NAME =
            MapreduceFileOutputCommitter.SUCCEEDED_FILE_NAME;
    static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
            MapreduceFileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;

    private static Path getOutputPath(JobContext context) {
        JobConf conf = context.getJobConf();
        return FileOutputFormat.getOutputPath(conf);
    }

    private static Path getOutputPath(TaskAttemptContext context) {
        JobConf conf = context.getJobConf();
        return FileOutputFormat.getOutputPath(conf);
    }

    private MapreduceFileOutputCommitter wrapped = null;

    private MapreduceFileOutputCommitter
        getWrapped(JobContext context) throws IOException {
            if(wrapped == null) {
                wrapped = new MapreduceFileOutputCommitter(getOutputPath(context), context);
            }
            return wrapped;
    }

    private MapreduceFileOutputCommitter
    getWrapped(TaskAttemptContext context) throws IOException {
        if(wrapped == null) {
            wrapped = new MapreduceFileOutputCommitter(
                    getOutputPath(context), context);
        }
        return wrapped;
    }

    /**
     * Compute the path where the output of a given job attempt will be placed.
     * @param context the context of the job.  This is used to get the
     * application attempt id.
     * @return the path to store job attempt data.
     */
    @Private
    Path getJobAttemptPath(JobContext context) {
        Path out = getOutputPath(context);
        return out == null ? null :
                MapreduceFileOutputCommitter.getJobAttemptPath(context, out);
    }

    @Private
    public Path getTaskAttemptPath(TaskAttemptContext context) throws IOException {
        Path out = getOutputPath(context);
        return out == null ? null : getTaskAttemptPath(context, out);
    }

    private Path getTaskAttemptPath(TaskAttemptContext context, Path out) throws IOException {
        Path workPath = FileOutputFormat.getWorkOutputPath(context.getJobConf());
        if(workPath == null && out != null) {
            return MapreduceFileOutputCommitter.getTaskAttemptPath(context, out);
        }
        return workPath;
    }

    /**
     * Compute the path where the output of a committed task is stored until
     * the entire job is committed.
     * @param context the context of the task attempt
     * @return the path where the output of a committed task is stored until
     * the entire job is committed.
     */
    @Private
    Path getCommittedTaskPath(TaskAttemptContext context) {
        Path out = getOutputPath(context);
        return out == null ? null :
                MapreduceFileOutputCommitter.getCommittedTaskPath(context, out);
    }

    public Path getWorkPath(TaskAttemptContext context, Path outputPath)
            throws IOException {
        return outputPath == null ? null : getTaskAttemptPath(context, outputPath);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        getWrapped(context).setupJob(context);
    }


    @Override
    public void commitJob(JobContext context) throws IOException {
//        if (hasOutputPath()) {
//            Path finalOutput = getOutputPath();
//            FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());
//            for(FileStatus stat: getAllCommittedTaskPaths(context)) {
//                mergePaths(fs, stat, finalOutput);
//            }
//
//            // delete the _temporary folder and create a _done file in the o/p folder
//            cleanupJob(context);
//            // True if the job requires output.dir marked on successful job.
//            // Note that by default it is set to true.
//            if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
//                Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
//                fs.create(markerPath).close();
//            }
//        } else {
//            LOG.warn("Output Path is null in commitJob()");
//        }
        getWrapped(context).commitJob(context);
    }

    @Override
    @Deprecated
    public void cleanupJob(JobContext context) throws IOException {
        getWrapped(context).cleanupJob(context);
    }

    @Override
    public void abortJob(JobContext context, int runState)
            throws IOException {
        JobStatus.State state;
        if(runState == JobStatus.State.RUNNING.getValue()) {
            state = JobStatus.State.RUNNING;
        } else if(runState == JobStatus.State.SUCCEEDED.getValue()) {
            state = JobStatus.State.SUCCEEDED;
        } else if(runState == JobStatus.State.FAILED.getValue()) {
            state = JobStatus.State.FAILED;
        } else if(runState == JobStatus.State.PREP.getValue()) {
            state = JobStatus.State.PREP;
        } else if(runState == JobStatus.State.KILLED.getValue()) {
            state = JobStatus.State.KILLED;
        } else {
            throw new IllegalArgumentException(runState+" is not a valid runState.");
        }
        getWrapped(context).abortJob(context, state);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).setupTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).commitTask(context, getTaskAttemptPath(context));
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).abortTask(context, getTaskAttemptPath(context));
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
        return getWrapped(context).needsTaskCommit(context, getTaskAttemptPath(context));
    }

    @Override
    public boolean isRecoverySupported() {
        return true;
    }

    @Override
    public void recoverTask(TaskAttemptContext context)
            throws IOException {
        getWrapped(context).recoverTask(context);
    }
}

