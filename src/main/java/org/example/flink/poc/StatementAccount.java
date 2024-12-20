package org.example.flink.poc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StatementAccount {
    private String accountNo;
    private String accountType;
    private String currency;
    private String branch;
    private String facility;
    private String ifscCode;
    private String micrCode;
    private String openingDate;
    private Double currentBalance;
    private Double currentODLimit;
    private Double drawingLimit;
    private String xnsStartDate;
    private String xnsEndDate;
    private List<Xn> xns;
}
