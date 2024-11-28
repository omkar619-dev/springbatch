package com.javalover.Entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "CUSTOMER_DETAILS")
public class Customer {

    @Id
    @GeneratedValue
    private int id;
    private String first_name;
    private String last_name;
    private String email;
    private String gender;
    private String ip_address;
}
