package org.apache.uniffle.common;
public final class ProjectConstants {
  /* Project version, specified in maven property. **/
  public static final String VERSION = "${project.version}";
  /* The latest git revision of at the time of building**/
  public static final String REVISION = "${git.revision}";

  private ProjectConstants() {} // prevent instantiation

  public static String getGitCommitId() {
    if (ProjectConstants.REVISION.length() >= 8) {
      return ProjectConstants.REVISION.substring(0, 8);
    }
    return ProjectConstants.REVISION;
  }
}