package org.example.flink.poc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Xn {
    private String txnId;
    private String mode;
    private String reference;
    private String date;
    private String chqNo;
    private String narration;
    private double amount;
    private String category;
    private double balance;
    private int index;
    private String type;

}
