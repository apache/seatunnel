package beanconfig;


import java.util.List;

public class EnumsConfig {
    public enum Problem {
        P1, P2, P3;
    };
    public enum Solution {
        S1, S2, S3;
    }
    Problem problem;
    List<Solution> solutions;

    public Problem getProblem() {
        return problem;
    }

    public void setProblem(Problem problem) {
        this.problem = problem;
    }

    public List<Solution> getSolutions() {
        return solutions;
    }

    public void setSolutions(List<Solution> solutions) {
        this.solutions = solutions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EnumsConfig)) {
            return false;
        }

        EnumsConfig that = (EnumsConfig) o;

        if (getProblem() != that.getProblem()) {
            return false;
        }
        return getSolutions() == that.getSolutions();

    }

    @Override
    public int hashCode() {
        int result = getProblem() != null ? getProblem().hashCode() : 0;
        result = 31 * result + (getSolutions() != null ? getSolutions().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("EnumsConfig{");
        sb.append("problem=").append(problem);
        sb.append(", solution=").append(solutions);
        sb.append('}');
        return sb.toString();
    }
}
