/**
 * 
 */
package com.yubaraj.olm;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.CannedAccessControlList;

/**
 * @author Yuba Raj Kalathoki
 *
 */
public class LifecycleConfiguration {
	public static AmazonS3Client s3Client;

	private void init() {
		s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	}

	private Rule addLifecycleRule(String bucketLifecycleApplicablePath) {
		Rule rule = new Rule().withId("Archive and then delete rule").withPrefix(bucketLifecycleApplicablePath)
				// .addTransition(
				// new
				// Transition().withDays(1).withStorageClass(StorageClass.StandardInfrequentAccess))
				// .addTransition(new
				// Transition().withDays(365).withStorageClass(StorageClass.Glacier))
				.withExpirationInDays(1).withStatus(BucketLifecycleConfiguration.ENABLED.toString());
		return rule;

	}

	private BucketLifecycleConfiguration configureRules(List<Rule> rules) {
		BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration().withRules(rules);
		return configuration;
	}

	/**
	 * Save configuration.
	 */
	private void saveConfiguration(String bucketName, BucketLifecycleConfiguration configuration) {
		s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
	}

	/**
	 * Updates permission for existing object key.
	 */
	private void setPermission(String bucketName, String keyName, CannedAccessControlList acl) {
		s3Client.setObjectAcl(bucketName, keyName, acl);
	}

	public static void main(String[] args) {
		LifecycleConfiguration classObj = new LifecycleConfiguration();
		String bucketName = "dev-test";
		String bucketLifecycleApplicablePath = "projectdocs/";
		String keyName = "projectdocs/yourfatherisprogrammer.png";
		try {
			classObj.init();

			// BucketLifecycleConfiguration.Rule rule1 = new
			// BucketLifecycleConfiguration.Rule()
			// .withId("Archive immediately rule").withPrefix("glacierobjects/")
			// .addTransition(new
			// Transition().withDays(0).withStorageClass(StorageClass.Glacier))
			// .withStatus(BucketLifecycleConfiguration.ENABLED.toString());

			Rule rule1 = classObj.addLifecycleRule(bucketLifecycleApplicablePath);

			BucketLifecycleConfiguration configuration = classObj.configureRules(Arrays.asList(rule1));

			classObj.saveConfiguration(bucketName, configuration);

			/**
			 * Delete configuration.
			 */
			// s3Client.deleteBucketLifecycleConfiguration(bucketName);

			/**
			 * Retrieve configuration.
			 */
			configuration = s3Client.getBucketLifecycleConfiguration(bucketName);
			/**
			 * Setting permission to keyName as public. So that user can read
			 * file before its expiration.
			 */
			if (configuration != null) {
				classObj.setPermission(bucketName, keyName, CannedAccessControlList.PublicRead);
			}

			/**
			 * Add a new rule.
			 */
			// configuration.getRules()
			// .add(new
			// BucketLifecycleConfiguration.Rule().withId("NewRule").withPrefix("YearlyDocuments/")
			// .withExpirationInDays(3650).withStatus(BucketLifecycleConfiguration.ENABLED.toString()));
			/**
			 * Save configuration.
			 */
			// s3Client.setBucketLifecycleConfiguration(bucketName,
			// configuration);

			/**
			 * Retrieve configuration.
			 */
			// configuration =
			// s3Client.getBucketLifecycleConfiguration(bucketName);

			/**
			 * Verify there are now three rules.
			 */
			// configuration =
			// s3Client.getBucketLifecycleConfiguration(bucketName);
			// System.out.format("Expected # of rules = 3; found: %s\n",
			// configuration.getRules().size());

			// System.out.println("Deleting lifecycle configuration. Next, we
			// verify deletion.");

			/**
			 * Retrieve nonexistent configuration.
			 */
			// configuration =
			// s3Client.getBucketLifecycleConfiguration(bucketName);
			String s = (configuration == null) ? "No configuration found." : "Configuration found.";
			System.out.println(s);

		} catch (AmazonS3Exception amazonS3Exception) {
			System.out.format("An Amazon S3 error occurred. Exception: %s", amazonS3Exception.toString());
		} catch (Exception ex) {
			System.out.format("Exception: %s", ex.toString());
		}
	}
}
