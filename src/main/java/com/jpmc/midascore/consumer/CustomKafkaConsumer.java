package com.jpmc.midascore.consumer;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.web.client.RestTemplate;


@Component
public class CustomKafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;


    public CustomKafkaConsumer(UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository,
                               RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }


    @KafkaListener(topics = "trader-updates", groupId = "midas-core")
    @Transactional
    public void listen(Transaction tx) {

        long senderId = tx.getSenderId();
        long recipientId = tx.getRecipientId();
        float amount = tx.getAmount();

        // Lookup users
        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        // Validation: users must exist
        if (sender == null || recipient == null) {
            return;
        }

        // Validation: sender must have enough balance
        if (sender.getBalance() < amount) {
            return;
        }
        Incentive incentive =
                restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        tx,
                        Incentive.class
                );

        float incentiveAmount =
                (incentive != null) ? incentive.getAmount() : 0;

        // Update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Store transaction record
        TransactionRecord record =
                new TransactionRecord(sender, recipient, amount, incentiveAmount);

        transactionRecordRepository.save(record);

    }
}
