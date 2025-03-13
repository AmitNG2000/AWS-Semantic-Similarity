import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.Arrays;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 2;
    protected static final String bucketName = "bucketassignment33";
    public static final String s3aPath = String.format("s3a://%s", bucketName);

    public static void main(String[] args) {
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to AWS");

        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        System.out.println("List cluster: " + emr.listClusters());

        try {
            // Step 1: Word Count
            HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                    .withJar(String.format("%s/jars/Step1.jar", s3aPath))
                    .withMainClass("Step1");

            StepConfig stepConfig1 = new StepConfig()
                    .withName("Step1")
                    .withHadoopJarStep(step1)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");


            // Step 2: Vector Construction and Similarity
            HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                    .withJar(String.format("%s/jars/Step2.jar", s3aPath))
                    .withMainClass("Step2");

            StepConfig stepConfig2 = new StepConfig()
                    .withName("Step2")
                    .withHadoopJarStep(step2)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");

            /*
            // Step 3
            HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                    .withJar(String.format("%s/jars/Step3.jar", s3Path))
                    .withMainClass("oldSteps.Step3");

            StepConfig stepConfig3 = new StepConfig()
                    .withName("Step3")
                    .withHadoopJarStep(step3)
                    .withActionOnFailure("TERMINATE_JOB_FLOW");
             */

            // Configure job flow
            JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                    .withInstanceCount(numberOfInstances)
                    .withMasterInstanceType(InstanceType.M4Large.toString())
                    .withSlaveInstanceType(InstanceType.M4Large.toString())
                    .withKeepJobFlowAliveWhenNoSteps(false)
                    .withPlacement(new PlacementType("us-east-1a"));

            RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                    .withName("Map reduce project")
                    .withInstances(instances)
                    .withSteps(Arrays.asList(stepConfig1, stepConfig2))
                    .withLogUri(String.format("%s/logs/", s3aPath))
                    .withServiceRole("EMR_DefaultRole")
                    .withJobFlowRole("EMR_EC2_DefaultRole")
                    .withReleaseLabel("emr-5.11.0");

            RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
            System.out.println("Ran job flow with id: " + runJobFlowResult.getJobFlowId());

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Failed to configure and run the job flow.");
        }
    }
}
