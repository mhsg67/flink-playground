package ca.mhsg.playground;


import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "test_keyspace", name = "test_table_pojo")
public class JEmployee {

    @PartitionKey
    @Column(name = "id")
    private Long employeeId;

    @Column(name = "name")
    private String employeeName;

    @Column(name = "title")
    private String employeeTitle;

    @Column(name = "age")
    private int employeeAge;

    public JEmployee(Long inId, String inName, String inTitle, int inAge) {
        this.employeeId = inId;
        this.employeeName = inName;
        this.employeeTitle = inTitle;
        this.employeeAge = inAge;
    }

    public Long getEmployeeId() {
        return this.employeeId;
    }

    public void setEmployeeId(Long employeeId) {
        this.employeeId = employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    public String getEmployeeTitle() {
        return employeeTitle;
    }

    public void setEmployeeTitle(String employeeTitle) {
        this.employeeTitle = employeeTitle;
    }

    public int getEmployeeAge() {
        return employeeAge;
    }

    public void setEmployeeAge(int employeeAge) {
        this.employeeAge = employeeAge;
    }
}
