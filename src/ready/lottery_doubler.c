/*
 * Lottery_doubler.c Hook - NFT based Lottery on the XRPL.
 *
 * Copyright (c) 2022 Chris Winkler.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define HAS_CALLBACK

#include <stdint.h>
#include "hookapi.h"

#define KEY_SIZE 32
#define ACCID_SIZE 20
#define ACC_DATA_SIZE 8

int64_t cbak(uint32_t reserved)
{
    uint8_t tx_failed = 1;
    TRACESTR("CBAK:");

    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Lottery CB: Could not slot originating txn."), oslot);

    // Meta data otxn
    int64_t mslot = meta_slot(0);
    if (mslot < 0)
        rollback(SBUF("Lottery CB: Could not slot meta data."), mslot);

    // Tx success
    int64_t tx_res_slot = slot_subfield(mslot, sfTransactionResult, 0);
    if (tx_res_slot < 0)
        rollback(SBUF("Lottery CB: Could not slot meta.sfTransactionResult"), tx_res_slot);
    uint8_t tx_res_buffer[1];
    slot(SBUF(tx_res_buffer), tx_res_slot);
    tx_failed = tx_res_buffer[0];
    if (tx_failed == 0)
        accept(SBUF("Lottery CB: Emitted Tx was tesSUCCESSful."), SUCCESS);

    uint8_t destination[ACCID_SIZE];
    int64_t destination_slot = slot_subfield(oslot, sfDestination, 0);
    if (destination_slot < 0)
        rollback(SBUF("Lottery CB: Could not slot otxn.sfDestination"), destination_slot);
    int64_t bw = slot(SBUF(destination), destination_slot);

    int64_t amt_slot = slot_subfield(oslot, sfAmount, 0);
    if (amt_slot < 0)
        rollback(SBUF("Lottery CB: Could not slot otxn.sfAmount."), NO_FREE_SLOTS);
    int64_t amt = slot_float(amt_slot);
    if (amt < 0)
        rollback(SBUF("Lottery CB: Could not parse amount."), PARSE_ERROR);
    uint64_t amount = float_int(amt, 6, 0);
    uint8_t amount_buf[8];
    UINT64_TO_BUF(amount_buf, amount);
    uint8_t acc[KEY_SIZE];
    for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        acc[i] = destination[i];
    if (state_set(SBUF(amount_buf), SBUF(acc)) != sizeof(amount_buf))
        rollback(SBUF("Lottery CB: could not write state_data_account"), INTERNAL_ERROR);
    accept(SBUF("Lottery CB: Stored failed Tx."), SUCCESS);
    return 0;
}

int64_t hook(uint32_t reserved)
{
    typedef struct
    {
        uint8_t *receiver;
        uint64_t amount;
    } Tx;
    Tx txs[2];
    uint8_t payout_address[] = "r9BjimZAz1a84k9eHnkRpPbv2aE6p1DThL";
    uint8_t payout_accid[ACCID_SIZE];
    util_accid(SBUF(payout_accid), SBUF(payout_address));
    uint8_t state_key_accid[KEY_SIZE];
    uint8_t state_data_accid[8];
    uint64_t sizes[3] = {10000000, 100000000, 1000000000};
    uint8_t num_of_txs = 0;

    // Accs
    uint8_t hook_accid[ACCID_SIZE];
    hook_account((uint32_t)hook_accid, ACCID_SIZE);
    if (hook_accid[0] == 0)
        rollback(SBUF("Lottery: Hook account field missing."), DOESNT_EXIST);
    uint8_t sender_accid[ACCID_SIZE];
    int32_t sender_accid_len = otxn_field(SBUF(sender_accid), sfAccount);
    if (sender_accid_len < ACCID_SIZE)
        rollback(SBUF("Lottery: sfAccount field missing."), DOESNT_EXIST);

    // Originating tx
    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Lottery: Could not slot originating txn."), NO_FREE_SLOTS);
    int64_t amt_slot = slot_subfield(oslot, sfAmount, 0);
    if (amt_slot < 0)
        rollback(SBUF("Lottery: Could not slot otxn.sfAmount."), NO_FREE_SLOTS);
    int64_t amt = slot_float(amt_slot);
    if (amt < 0)
        rollback(SBUF("Lottery: Could not parse amount."), PARSE_ERROR);
    uint64_t amount_in = float_int(amt, 6, 0);
    int64_t is_xrp = slot_type(amt_slot, 1);
    if (is_xrp < 0)
        rollback(SBUF("Lottery: Could not determine sent amount type."), PARSE_ERROR);
    if (is_xrp != 1)
        rollback(SBUF("Lottery: IOU not supported."), INVALID_ARGUMENT);
    if (amount_in == sizes[0] && amount_in == sizes[1] && amount_in == sizes[2])
        rollback(SBUF("Lottery: Invalid Amount sent."), TOO_BIG);
    int64_t dest_tag_slot = slot_subfield(oslot, sfDestinationTag, 0);
    uint8_t dest_tag_buf[4];
    int64_t bw = slot(SBUF(dest_tag_buf), dest_tag_slot);
    uint32_t destination_tag = UINT32_FROM_BUF(dest_tag_buf);
    if (destination_tag == 0) // gamble
    {
        uint8_t nonce[KEY_SIZE];
        etxn_nonce(SBUF(nonce));
        uint8_t p_random_number = nonce[15];
        TRACEVAR(p_random_number);
        if (p_random_number % 10 < 6)
            accept(SBUF("Lottery: You lost."), SUCCESS);

        txs[0].receiver = sender_accid;
        txs[0].amount = amount_in * 2;
        ++num_of_txs;
    }
    else if (destination_tag == 255) // retry
    {
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
            state_key_accid[i] = sender_accid[i];
        if (state(SBUF(state_data_accid), SBUF(state_key_accid)) != sizeof(state_data_accid))
            rollback(SBUF("Lottery: No open payments."), DOESNT_EXIST);
        txs[0].receiver = sender_accid;
        txs[0].amount = UINT64_FROM_BUF(state_data_accid);
        ++num_of_txs;
        if (state_set(0, 0, SBUF(state_key_accid)) < 0)
            rollback(SBUF("Lottery: could not delete state_data_accid"), INTERNAL_ERROR);
    }
    else if (destination_tag > 255) // payout
    {
        uint8_t equal = 0;
        BUFFER_EQUAL(equal, sender_accid, payout_accid, ACCID_SIZE);
        if (equal != 1)
            rollback(SBUF("Lottery: Wrong account"), INVALID_ARGUMENT);
        txs[0].receiver = payout_accid;
        txs[0].amount = (uint64_t)destination_tag * 1000000;
        ++num_of_txs;
    }
    else
        rollback(SBUF("Lottery: Invalid Destination Tag."), INVALID_ARGUMENT);

    //  Submit tx(s)
    etxn_reserve(num_of_txs);
    uint8_t emithash[32];
    int64_t e = 0;
    for (int i = 0; GUARD(2), i < num_of_txs; ++i)
    {
        unsigned char tx[PREPARE_PAYMENT_SIMPLE_SIZE];
        PREPARE_PAYMENT_SIMPLE(tx, txs[i].amount, txs[i].receiver, i + 1, 0);
        e = emit(SBUF(emithash), SBUF(tx));
        if (e < 0)
            rollback(SBUF("Lottery: Failed to emit XRP!"), e);
    }

    accept(SBUF("Lottery: Everything worked as expected."), 1);
    return 0;
}