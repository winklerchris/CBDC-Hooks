/*
 * loan.c Hook - P2P Loans on the XRPL.
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

#undef PREPARE_PAYMENT_SIMPLE_TRUSTLINE_SIZE
#define PREPARE_PAYMENT_SIMPLE_TRUSTLINE_SIZE 310
// Instead of PREPARE_PAYMENT_SIMPLE_TRUSTLINE_LOOP
#undef ENCODE_TL
#define ENCODE_TL(buf_out, tlamt, amount_type)                   \
    {                                                            \
        uint8_t uat = amount_type;                               \
        buf_out[0] = 0x60U + (uat & 0x0FU);                      \
        *(uint64_t *)(buf_out + 1) = *(uint64_t *)(tlamt + 0);   \
        *(uint64_t *)(buf_out + 9) = *(uint64_t *)(tlamt + 8);   \
        *(uint64_t *)(buf_out + 17) = *(uint64_t *)(tlamt + 16); \
        *(uint64_t *)(buf_out + 25) = *(uint64_t *)(tlamt + 24); \
        *(uint64_t *)(buf_out + 33) = *(uint64_t *)(tlamt + 32); \
        *(uint64_t *)(buf_out + 41) = *(uint64_t *)(tlamt + 40); \
        buf_out += ENCODE_TL_SIZE;                               \
    }

#define MAX_MEMO_SIZE 4096
#define STATE_DATA_SIZE 85
#define MEMO_DATA_SIZE_OPEN 58
#define MEMO_DATA_SIZE 65
#define MAX_CURRENCIES 6
#define MAX_TRANSACTIONS 3
#define MAX_STATES 1000
#define STATE_COUNTER_KEY_END 7
#define FEE_STATE_KEY_END 8
#define FAILED_STATE_KEY_END 9
#define KEY_SIZE 32
#define ACCID_SIZE 20
#define FEE_PERCENT 1000 // *0.001
#define MIN_FEE 10000000 // 10
#define MAX_WAITING_TIME 60 * 60 * 24 * 31

#define MEMO_ACTION_OFFSET 0
#define MEMO_ROLE_OFFSET 1
#define MEMO_LOAN_CURRENCY_OFFSET 2
#define MEMO_LOAN_AMOUNT_OFFSET 5
#define MEMO_COLLATERAL_CURRENCY_OFFSET 25
#define MEMO_COLLATERAL_AMOUNT_OFFSET 28
#define MEMO_INTEREST_RATE_OFFSET 48
#define MEMO_LOAN_PERIOD_OFFSET 53

#define LOAN_STATE_OFFSET 0
#define ROLE_OFFSET 1
#define LOAN_RETURNED_OFFSET 2
#define LOAN_CURRENCY_OFFSET 3
#define COLLATERAL_CURRENCY_OFFSET 4
#define LOAN_PERIOD_OFFSET 5
#define INTEREST_RATE_OFFSET 9
#define LOAN_AMOUNT_OFFSET 13
#define COLLATERAL_AMOUNT_OFFSET 21
#define INTEREST_OFFSET 29
#define TIMESTAMP_END_OFFSET 37
#define MAKER_ACCID_OFFSET 45
#define TAKER_ACCID_OFFSET 65

int64_t cbak(uint32_t reserved)
{
    // Originating tx
    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Loan CB: Could not slot originating txn."), NO_FREE_SLOTS);

    // Add fee paid
    uint8_t fee_state_key[KEY_SIZE];
    fee_state_key[31] = FEE_STATE_KEY_END;
    int8_t fee_state_data[8];
    int64_t fee_slot = slot_subfield(oslot, sfFee, 0);
    if (fee_slot < 0)
        rollback(SBUF("Loan CB: Could not slot otxn.sfFee"), NO_FREE_SLOTS);
    int64_t fee = slot_float(fee_slot);
    state(SBUF(fee_state_data), SBUF(fee_state_key));
    int64_t fee_sum = float_sto_set(SBUF(fee_state_data));
    fee_sum = float_sum(fee_sum, fee);
    if (float_sto(SBUF(fee_state_data), 0, 0, 0, 0, fee_sum, -1) < 0)
        rollback(SBUF("Loan CB: Could not dump fee_sum into sto"), NOT_AN_AMOUNT);
    if (state_set(SBUF(fee_state_data), SBUF(fee_state_key)) != 8)
        rollback(SBUF("Loan CB: could not write fee_state"), INTERNAL_ERROR);

    // Tx result
    int64_t mslot = meta_slot(0);
    if (mslot < 0)
        rollback(SBUF("Loan CB: Could not slot meta data."), NO_FREE_SLOTS);
    int64_t tx_slot = slot_subfield(mslot, sfTransactionResult, 0);
    if (tx_slot < 0)
        rollback(SBUF("Loan CB: Could not slot meta.sfTransactionResult"), NO_FREE_SLOTS);
    uint8_t res_buffer[1];
    slot(SBUF(res_buffer), tx_slot);
    TRACEHEX(res_buffer);
    if (res_buffer[0] == 0)
        accept(SBUF("Loan CB: Emitted Tx was tesSUCCESS."), 1);

    // Failed Tx
    uint8_t failed_state_key[KEY_SIZE];
    ledger_nonce(SBUF(failed_state_key));
    failed_state_key[31] = FAILED_STATE_KEY_END;
    uint8_t failed_state_data[STATE_DATA_SIZE];
    uint8_t *failed_state_data_ptr = failed_state_data;
    uint8_t destination_accid[ACCID_SIZE];
    int32_t destination_accid_len = otxn_field(SBUF(destination_accid), sfDestination);
    if (destination_accid_len < ACCID_SIZE)
        rollback(SBUF("Loan CB: sfDestination field missing."), DOESNT_EXIST);
    int64_t amt_slot = slot_subfield(oslot, sfAmount, 0);
    if (amt_slot < 0)
        rollback(SBUF("Loan CB: Could not slot otxn.sfAmount"), NO_FREE_SLOTS);
    uint8_t otxn_buffer[48];
    slot(SBUF(otxn_buffer), amt_slot);
    int64_t is_xrp = slot_type(amt_slot, 1);
    if (is_xrp < 0)
        rollback(SBUF("Loan CB: Could not determine sent amount type"), PARSE_ERROR);
    for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; i++)
        failed_state_data[i] = destination_accid[i];
    failed_state_data[20] = is_xrp == 1 ? 1 : 0;
    if (is_xrp == 1)
    {
        int64_t amt = slot_float(amt_slot);
        if (amt < 0)
            rollback(SBUF("Loan: Could not parse amount."), PARSE_ERROR);
        if (float_sto((failed_state_data_ptr + 21), 8, 0, 0, 0, 0, amt, -1) != 8)
            rollback(SBUF("Loan CB: Could not dump sfAmount-XRP"), NOT_AN_AMOUNT);
    }
    else
    {
        if (slot((failed_state_data_ptr + 21), 48, amt_slot) != 48)
            rollback(SBUF("Loan CB: Could not dump sfAmount-IOU"), NOT_AN_AMOUNT);
    }

    if (state_set(SBUF(failed_state_data), SBUF(failed_state_key)) != STATE_DATA_SIZE)
        rollback(SBUF("Loan CB: could not write state"), INTERNAL_ERROR);

    // Amount of stored states
    uint8_t state_counter_key[KEY_SIZE];
    state_counter_key[31] = STATE_COUNTER_KEY_END;
    uint8_t state_counter_data[8];
    state(SBUF(state_counter_data), SBUF(state_counter_key));
    uint64_t state_counter = UINT64_FROM_BUF(state_counter_data);
    ++state_counter;
    UINT64_TO_BUF(state_counter_data, state_counter);
    if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
        rollback(SBUF("Loan CB: could not write state_counter"), INTERNAL_ERROR);

    accept(SBUF("Loan CB: Stored failed Tx."), 1);
    return 0;
}

int64_t hook(uint32_t reserved)
{
    enum OfferState
    {
        waiting = 1,
        running = 2
    };
    enum ReturnState
    {
        returned = 1,
        not_returned = 2
    };
    enum Action
    {
        make = 1,
        cancel = 2,
        take = 3,
        repay = 4,
        close = 5,
        resend = 6
    };
    enum Role
    {
        borrower = 1,
        lender = 2
    };
    int equal = 0;
    int64_t time = 0;
    uint8_t txq = 0;
    uint8_t currency_in = 0;
    uint8_t earnings_accid[ACCID_SIZE];
    util_accid(earnings_accid, ACCID_SIZE, SBUF("rfmjnpZxyRddKWPmb52S5P4NtMWAGgoqDd"));
    uint8_t issuer_accids[MAX_CURRENCIES][ACCID_SIZE];
    util_accid(issuer_accids[1], ACCID_SIZE, SBUF("rMf4L4T4Dkp1VHTsSqJ5KUeGjt44rNHMSt"));
    util_accid(issuer_accids[2], ACCID_SIZE, SBUF("rMf4L4T4Dkp1VHTsSqJ5KUeGjt44rNHMSt"));
    util_accid(issuer_accids[3], ACCID_SIZE, SBUF("rMf4L4T4Dkp1VHTsSqJ5KUeGjt44rNHMSt"));
    util_accid(issuer_accids[4], ACCID_SIZE, SBUF("rMf4L4T4Dkp1VHTsSqJ5KUeGjt44rNHMSt"));
    util_accid(issuer_accids[5], ACCID_SIZE, SBUF("rMf4L4T4Dkp1VHTsSqJ5KUeGjt44rNHMSt"));
    uint8_t c0[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'X', 'R', 'P', 0, 0, 0, 0, 0};
    uint8_t c1[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'G', 'B', 'P', 0, 0, 0, 0, 0};
    uint8_t c2[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'E', 'U', 'R', 0, 0, 0, 0, 0};
    uint8_t c3[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'U', 'S', 'D', 0, 0, 0, 0, 0};
    uint8_t c4[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'C', 'H', 'F', 0, 0, 0, 0, 0};
    uint8_t c5[ACCID_SIZE] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'C', 'N', 'H', 0, 0, 0, 0, 0};
    uint8_t currencies[MAX_CURRENCIES][ACCID_SIZE];
    for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; i++)
    {
        currencies[0][i] = c0[i];
        currencies[1][i] = c1[i];
        currencies[2][i] = c2[i];
        currencies[3][i] = c3[i];
        currencies[4][i] = c4[i];
        currencies[5][i] = c5[i];
    }
    struct tx
    {
        uint8_t *receiver;
        uint8_t currency;
        uint64_t amount;
    };
    struct tx txs[MAX_TRANSACTIONS];
    uint8_t state_key[KEY_SIZE];
    uint8_t *state_key_ptr = state_key;
    uint8_t loan_id[KEY_SIZE];
    uint8_t state_data[STATE_DATA_SIZE];
    uint8_t *state_data_ptr = state_data;
    uint8_t maker_accid[ACCID_SIZE];
    uint8_t taker_accid[ACCID_SIZE];

    uint8_t action = 0;
    uint8_t role = 0;
    uint8_t loan_currency = 0;
    uint8_t collateral_currency = 0;
    uint32_t loan_period = 0;
    uint32_t interest_rate = 0;
    uint64_t loan_amount = 0;
    uint64_t collateral_amount = 0;
    uint64_t interest = 0;
    uint64_t timestamp_end = 0;
    uint64_t fee = 0;

    uint8_t state_counter_key[KEY_SIZE];
    state_counter_key[31] = STATE_COUNTER_KEY_END;
    uint8_t state_counter_data[8];
    state(SBUF(state_counter_data), SBUF(state_counter_key));
    uint64_t state_counter = UINT64_FROM_BUF(state_counter_data);

    unsigned char hook_accid[ACCID_SIZE];
    hook_account((uint32_t)hook_accid, ACCID_SIZE);
    if (hook_accid[0] == 0)
        rollback(SBUF("Loan: Hook account field missing."), DOESNT_EXIST);

    uint8_t sender_accid[ACCID_SIZE];
    int32_t sender_accid_len = otxn_field(SBUF(sender_accid), sfAccount);
    if (sender_accid_len < ACCID_SIZE)
        rollback(SBUF("Loan: sfAccount field missing."), DOESNT_EXIST);

    // Originating tx
    int64_t oslot = otxn_slot(0);
    if (oslot < 0)
        rollback(SBUF("Loan: Could not slot originating txn."), NO_FREE_SLOTS);
    int64_t amt_slot = slot_subfield(oslot, sfAmount, 0);
    if (amt_slot < 0)
        rollback(SBUF("Loan: Could not slot otxn.sfAmount"), NO_FREE_SLOTS);
    int64_t amt = slot_float(amt_slot);
    if (amt < 0)
        rollback(SBUF("Loan: Could not parse amount."), PARSE_ERROR);
    uint64_t amount_in = float_int(amt, 6, 0);
    int64_t is_xrp = slot_type(amt_slot, 1);
    if (is_xrp < 0)
        rollback(SBUF("Loan: Could not determine sent amount type"), PARSE_ERROR);
    if (is_xrp != 1)
    {
        uint8_t amount_buffer[48];
        if (slot(SBUF(amount_buffer), amt_slot) < 48)
            rollback(SBUF("Loan: Could not dump sfAmount"), NOT_AN_AMOUNT);
        TRACEHEX(amount_buffer);
        uint8_t *currency_ptr = amount_buffer;
        equal = 0;
        int c;
        for (c = 0; GUARD(MAX_CURRENCIES), c < MAX_CURRENCIES && equal != 1; ++c)
            BUFFER_EQUAL_GUARD(equal, currency_ptr + 8, ACCID_SIZE, currencies[c], ACCID_SIZE, MAX_CURRENCIES);
        if (equal == 1)
            currency_in = c - 1;
        else
            rollback(SBUF("Loan: IOU not supported."), INVALID_ARGUMENT);
    }

    // Memo
    uint8_t memos[MAX_MEMO_SIZE];
    int64_t memos_len = otxn_field(SBUF(memos), sfMemos);
    if (memos_len <= 0)
        rollback(SBUF("Loan: Incoming txn has no memo provided."), DOESNT_EXIST);
    int64_t memo_lookup = sto_subarray((uint32_t)memos, memos_len, 0);
    uint8_t *memo_ptr = SUB_OFFSET(memo_lookup) + memos;
    uint32_t memo_len = SUB_LENGTH(memo_lookup);
    memo_lookup = sto_subfield((uint32_t)memo_ptr, memo_len, sfMemo);
    memo_ptr = SUB_OFFSET(memo_lookup) + memo_ptr;
    memo_len = SUB_LENGTH(memo_lookup);
    if (memo_lookup < 0)
        rollback(SBUF("Loan: Incoming txn had a blank sfMemos, abort."), DOESNT_EXIST);
    int64_t format_lookup = sto_subfield((uint32_t)memo_ptr, memo_len, sfMemoFormat);
    uint8_t *format_ptr = SUB_OFFSET(format_lookup) + memo_ptr;
    uint32_t format_len = SUB_LENGTH(format_lookup);
    int is_unsigned_payload = 0;
    BUFFER_EQUAL_STR_GUARD(is_unsigned_payload, format_ptr, format_len, "text/plain", 1);
    if (!is_unsigned_payload)
        rollback(SBUF("Loan: Memo is an invalid format."), DOESNT_EXIST);
    int64_t type_lookup = sto_subfield((uint32_t)memo_ptr, memo_len, sfMemoType);
    uint8_t *type_ptr = SUB_OFFSET(type_lookup) + memo_ptr;
    uint32_t type_len = SUB_LENGTH(type_lookup);
    is_unsigned_payload = 0;
    BUFFER_EQUAL_STR_GUARD(is_unsigned_payload, type_ptr, type_len, "Description", 1);
    if (!is_unsigned_payload)
        rollback(SBUF("Loan: Memo has invalid type."), DOESNT_EXIST);
    int64_t data_lookup = sto_subfield((uint32_t)memo_ptr, memo_len, sfMemoData);
    uint8_t *data_ptr = SUB_OFFSET(data_lookup) + memo_ptr;
    uint32_t data_len = SUB_LENGTH(data_lookup);
    action = data_ptr[MEMO_ACTION_OFFSET] - '0';
    if (action < make || action > resend)
        rollback(SBUF("Loan: Invalid action."), OUT_OF_BOUNDS);
    if ((action == make && data_len != MEMO_DATA_SIZE_OPEN) || (action != make && data_len != MEMO_DATA_SIZE))
        rollback(SBUF("Loan: Invalid memo data length."), TOO_BIG);

    // Parse memo if not "make"
    if (action != make)
    {
        int x = 0;
        for (int i = 0; GUARD(KEY_SIZE), i < KEY_SIZE && x < KEY_SIZE * 2; ++i)
        {
            loan_id[i] = ((data_ptr[x + 1] - (data_ptr[x + 1] >= 65 ? '7' : '0')) * 16) + (data_ptr[x + 2] - (data_ptr[x + 2] >= 65 ? '7' : '0'));
            x += 2;
        }
        if (state(SBUF(state_data), SBUF(loan_id)) != STATE_DATA_SIZE)
            rollback(SBUF("Loan: Loan does not exist"), DOESNT_EXIST);
        if (loan_id[31] == 0)
        {
            role = state_data_ptr[ROLE_OFFSET];
            if (role < borrower || role > lender)
                rollback(SBUF("Loan: Invalid role."), DOESNT_EXIST);
        }
    }
    switch (action)
    {
    case make:
        if (state_counter > MAX_STATES)
            rollback(SBUF("Loan: No new offer are accepted."), TOO_BIG);
        ++state_counter;
        UINT64_TO_BUF(state_counter_data, state_counter);
        if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
            rollback(SBUF("Loan: could not write state_counter"), INTERNAL_ERROR);

        // Parse memo
        role = data_ptr[MEMO_ROLE_OFFSET] - '0';
        if (role < borrower || role > lender)
            rollback(SBUF("Loan: Invalid role."), DOESNT_EXIST);
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        {
            if (data_ptr[MEMO_LOAN_CURRENCY_OFFSET + i] - '0' < 0 || data_ptr[MEMO_LOAN_CURRENCY_OFFSET + i] - '0' > 39)
                rollback(SBUF("Loan: Invalid memo data (0-9)."), TOO_BIG);
            if (i < MEMO_LOAN_AMOUNT_OFFSET - MEMO_LOAN_CURRENCY_OFFSET)
            {
                if (loan_currency > 24)
                    rollback(SBUF("Loan: loan_currency overflow."), OUT_OF_BOUNDS);
                loan_currency = loan_currency * 10 + data_ptr[i + MEMO_LOAN_CURRENCY_OFFSET] - '0';
                if (collateral_currency > 24)
                    rollback(SBUF("Loan: collateral_currency overflow."), OUT_OF_BOUNDS);
                collateral_currency = collateral_currency * 10 + data_ptr[i + MEMO_COLLATERAL_CURRENCY_OFFSET] - '0';
            }
            if (i < MEMO_LOAN_PERIOD_OFFSET - MEMO_INTEREST_RATE_OFFSET)
            {
                if (interest_rate > 429496728)
                    rollback(SBUF("Loan: interest_rate overflow."), OUT_OF_BOUNDS);
                interest_rate = interest_rate * 10 + data_ptr[i + MEMO_INTEREST_RATE_OFFSET] - '0';
                if (loan_period > 429496728)
                    rollback(SBUF("Loan: loan_period overflow."), OUT_OF_BOUNDS);
                loan_period = loan_period * 10 + data_ptr[i + MEMO_LOAN_PERIOD_OFFSET] - '0';
            }
            if (loan_amount > 1844674407370955160)
                rollback(SBUF("Loan: loan_amount overflow."), OUT_OF_BOUNDS);
            loan_amount = loan_amount * 10 + data_ptr[i + MEMO_LOAN_AMOUNT_OFFSET] - '0';
            if (collateral_amount > 1844674407370955160)
                rollback(SBUF("Loan: collateral_amount overflow."), OUT_OF_BOUNDS);
            collateral_amount = collateral_amount * 10 + data_ptr[i + MEMO_COLLATERAL_AMOUNT_OFFSET] - '0';
        }

        // Loan conditions validity
        if (loan_currency < 0 || loan_currency > MAX_CURRENCIES)
            rollback(SBUF("Loan: Invalid loan_currency (0-MAX_CURRENCIES)"), OUT_OF_BOUNDS);
        if (collateral_currency < 0 || collateral_currency > MAX_CURRENCIES)
            rollback(SBUF("Loan: Invalid collateral_currency (0-MAX_CURRENCIES)"), OUT_OF_BOUNDS);
        if (loan_period < 1 || loan_period > 9999)
            rollback(SBUF("Loan: Invalid loan_period (0.001 - 9.999)"), OUT_OF_BOUNDS);
        if (interest_rate < 1 || interest_rate > 99999)
            rollback(SBUF("Loan: Invalid interest_rate (0.001 - 99.999)"), OUT_OF_BOUNDS);
        if (loan_amount < 1000)
            rollback(SBUF("Loan: Invalid loan_amount (min 0.0001)"), OUT_OF_BOUNDS);
        if (collateral_amount < 1000)
            rollback(SBUF("Loan: Invalid collateral_amount (min 0.0001)"), OUT_OF_BOUNDS);
        if ((uint64_t)interest_rate * (uint64_t)loan_period / (uint64_t)365 > 90000)
            rollback(SBUF("Loan: Invalid interest (Maximum 90.000)"), OUT_OF_BOUNDS);

        // Trustline check
        if ((role == borrower ? loan_currency : collateral_currency) != 0)
        {
            uint8_t keylet[34];
            if (util_keylet(SBUF(keylet), KEYLET_LINE, SBUF(issuer_accids[(role == borrower ? loan_currency : collateral_currency)]), SBUF(sender_accid),
                            SBUF(currencies[(role == borrower ? loan_currency : collateral_currency)])) != 34)
                rollback(SBUF("Loan: Internal error, could not generate keylet"), NO_SUCH_KEYLET);
            int64_t user_loan_trustline_slot = slot_set(SBUF(keylet), 0);
            if (user_loan_trustline_slot < 0)
                rollback(SBUF("Loan: You must have a trustline set for IOU to this account."), NO_SUCH_KEYLET);
            int compare_result = 0;
            ACCOUNT_COMPARE(compare_result, issuer_accids[(role == borrower ? loan_currency : collateral_currency)], sender_accid);
            if (compare_result == 0)
                rollback(SBUF("Loan: Invalid trustline set hi=lo?"), DOESNT_EXIST);
            int64_t lim_slot = slot_subfield(user_loan_trustline_slot, (compare_result > 0 ? sfLowLimit : sfHighLimit), 0);
            if (lim_slot < 0)
                rollback(SBUF("Loan: Could not find sfLowLimit."), DOESNT_EXIST);
            int64_t user_trustline_limit = slot_float(lim_slot);
            if (user_trustline_limit < 0)
                rollback(SBUF("Loan: Could not parse user trustline limit"), PARSE_ERROR);
            if (float_compare(user_trustline_limit, float_set(-6, (role == borrower ? loan_amount : collateral_amount)), COMPARE_GREATER) != 1)
                rollback(SBUF("Loan: You must set a trustline for IOU greater than the wanted amount."), TOO_SMALL);
        }

        // Prepare state
        state_data_ptr[LOAN_STATE_OFFSET] = waiting;
        state_data_ptr[ROLE_OFFSET] = role;
        state_data_ptr[LOAN_RETURNED_OFFSET] = not_returned;
        state_data_ptr[LOAN_CURRENCY_OFFSET] = loan_currency;
        state_data_ptr[COLLATERAL_CURRENCY_OFFSET] = collateral_currency;
        interest = (uint64_t)((double)loan_period * (double)interest_rate * (double)collateral_amount / (double)100000 / (double)365);
        fee = (role == borrower ? collateral_amount : loan_amount) / FEE_PERCENT;
        fee = fee > MIN_FEE ? fee : MIN_FEE;
        if (amount_in - fee < (role == borrower ? collateral_amount : loan_amount))
            rollback(SBUF("Loan: Not enough money sent."), TOO_SMALL);
        UINT32_TO_BUF(state_data_ptr + LOAN_PERIOD_OFFSET, loan_period);
        UINT32_TO_BUF(state_data_ptr + INTEREST_RATE_OFFSET, interest_rate);
        UINT64_TO_BUF(state_data_ptr + LOAN_AMOUNT_OFFSET, loan_amount);
        UINT64_TO_BUF(state_data_ptr + COLLATERAL_AMOUNT_OFFSET, collateral_amount);
        UINT64_TO_BUF(state_data_ptr + INTEREST_OFFSET, interest);
        time = ledger_last_time();
        if (time < 1)
            rollback(SBUF("Loan: Could not retrieve last ledger time!"), DOESNT_EXIST);
        timestamp_end = (uint64_t)time + (uint64_t)MAX_WAITING_TIME;
        UINT64_TO_BUF(state_data_ptr + TIMESTAMP_END_OFFSET, timestamp_end);
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        {
            state_data_ptr[i + MAKER_ACCID_OFFSET] = sender_accid[i];
            state_data_ptr[i + TAKER_ACCID_OFFSET] = 0;
        }
        int32_t seq_len = otxn_field(state_key_ptr + 8, 4, sfSequence);
        if (seq_len < 0)
            rollback(SBUF("Loan: sfSequence field missing."), DOESNT_EXIST);
        UINT64_TO_BUF(state_key_ptr, time);

        // Set state
        if (state_set(SBUF(state_data), SBUF(state_key)) != STATE_DATA_SIZE)
            rollback(SBUF("Loan: Could not write state!"), INTERNAL_ERROR);

        // Prepare tx
        txs[0].receiver = earnings_accid;
        txs[0].amount = fee;
        txs[0].currency = role == borrower ? collateral_currency : loan_currency;
        txq = 1;
        if (txs[0].currency == 0)
        {
            uint8_t fee_state_key[KEY_SIZE];
            fee_state_key[31] = FEE_STATE_KEY_END;
            int8_t fee_state_data[8];
            state(SBUF(fee_state_data), SBUF(fee_state_key));
            int64_t fee_sum = float_sto_set(SBUF(fee_state_data));
            if (float_compare(fee_sum, float_set(6, 10), COMPARE_GREATER) == 1)
            {
                fee_sum = float_sum(fee_sum, float_negate((float_set(-6, fee))));
                if (float_sto(SBUF(fee_state_data), 0, 0, 0, 0, fee_sum, -1) < 0)
                    rollback(SBUF("Loan: Could not dump fee_sum into sto"), NOT_AN_AMOUNT);
                if (state_set(SBUF(fee_state_data), SBUF(fee_state_key)) != 8)
                    rollback(SBUF("Loan: could not write fee_state"), INTERNAL_ERROR);
                txq = 0;
            }
        }

        TRACESTR("Loan: Make");
        break;
    case cancel:
        // Check if loan can be cancelled
        state_counter -= state_counter > 0 ? 1 : 0;
        UINT64_TO_BUF(state_counter_data, state_counter);
        if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
            rollback(SBUF("Loan: could not write state_counter"), INTERNAL_ERROR);
        if (state_data_ptr[LOAN_STATE_OFFSET] != waiting)
            rollback(SBUF("Loan: Loan is not in Waiting state"), INVALID_ARGUMENT);
        if (amount_in > 1000000)
            rollback(SBUF("Loan: Too much currency sent!"), TOO_BIG);
        timestamp_end = UINT64_FROM_BUF(state_data_ptr + TIMESTAMP_END_OFFSET);
        time = ledger_last_time();
        if (time < 1)
            rollback(SBUF("Loan: Could not retrieve last ledger time!"), INTERNAL_ERROR);
        equal = 0;
        BUFFER_EQUAL(equal, state_data_ptr + MAKER_ACCID_OFFSET, sender_accid, ACCID_SIZE);
        if (equal != 1 && time < timestamp_end)
            rollback(SBUF("Loan: Only Maker can cancel the offer"), INVALID_ARGUMENT);

        // Prepare tx
        txs[0].receiver = sender_accid;
        collateral_amount = UINT64_FROM_BUF(state_data_ptr + (role == borrower ? COLLATERAL_AMOUNT_OFFSET : LOAN_AMOUNT_OFFSET));
        txs[0].amount = collateral_amount;
        txs[0].currency = state_data_ptr[(role == borrower ? COLLATERAL_CURRENCY_OFFSET : LOAN_CURRENCY_OFFSET)];
        txq = 1;
        if (state_set(0, 0, SBUF(loan_id)) < 0)
            rollback(SBUF("Loan: Could not reset loan"), INTERNAL_ERROR);

        TRACESTR("Loan: Cancel");
        break;
    case take:
        // Check if loan can be taken
        if (state_data_ptr[LOAN_STATE_OFFSET] != waiting)
            rollback(SBUF("Loan: Loan is not available"), INVALID_ARGUMENT);
        equal = 0;
        BUFFER_EQUAL(equal, state_data_ptr + MAKER_ACCID_OFFSET, sender_accid, ACCID_SIZE);
        if (equal != 0)
            rollback(SBUF("Loan: Maker can not be Taker"), INVALID_ARGUMENT);
        if (state_data_ptr[(role == borrower ? LOAN_CURRENCY_OFFSET : COLLATERAL_CURRENCY_OFFSET)] != currency_in)
            rollback(SBUF("Loan: Wrong currency sent!"), INVALID_ARGUMENT);
        time = ledger_last_time();
        if (time < 1)
            rollback(SBUF("Loan: Could not retrieve last ledger time!"), INTERNAL_ERROR);
        loan_period = UINT32_FROM_BUF(state_data_ptr + LOAN_PERIOD_OFFSET);
        UINT64_TO_BUF(state_data_ptr + TIMESTAMP_END_OFFSET, ((uint64_t)loan_period * 24 * 60 * 60 + time));

        uint64_t amt = UINT64_FROM_BUF(state_data_ptr + (role == borrower ? LOAN_AMOUNT_OFFSET : COLLATERAL_AMOUNT_OFFSET));
        if (amount_in < amt)
            rollback(SBUF("Loan: Not enough currency sent!"), TOO_SMALL);

        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        {
            maker_accid[i] = state_data_ptr[i + MAKER_ACCID_OFFSET];
            state_data_ptr[i + TAKER_ACCID_OFFSET] = sender_accid[i];
        }
        state_data_ptr[LOAN_STATE_OFFSET] = running;

        if (state_set(SBUF(state_data), SBUF(loan_id)) != STATE_DATA_SIZE)
            rollback(SBUF("Loan: Could not write state!"), INTERNAL_ERROR);

        // Prepare tx
        txs[0].receiver = role == borrower ? maker_accid : sender_accid;
        txs[0].amount = UINT64_FROM_BUF(state_data_ptr + LOAN_AMOUNT_OFFSET);
        txs[0].currency = state_data_ptr[LOAN_CURRENCY_OFFSET];
        txq = 1;

        TRACESTR("Loan: Take");
        break;
    case repay:
        // Check if loan can be repaid
        state_counter -= state_counter > 0 ? 1 : 0;
        UINT64_TO_BUF(state_counter_data, state_counter);
        if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
            rollback(SBUF("Loan: could not write state_counter"), INTERNAL_ERROR);
        if (state_data_ptr[LOAN_STATE_OFFSET] != running)
            rollback(SBUF("Loan: Loan can not be repaid"), INVALID_ARGUMENT);
        equal = 0;
        BUFFER_EQUAL(equal, state_data_ptr + (role == borrower ? MAKER_ACCID_OFFSET : TAKER_ACCID_OFFSET), sender_accid, ACCID_SIZE);
        if (equal == 0)
            rollback(SBUF("Loan: Only Borrower can repay the loan"), INVALID_ARGUMENT);
        if (state_data_ptr[LOAN_CURRENCY_OFFSET] != currency_in)
            rollback(SBUF("Loan: Wrong currency sent!"), INVALID_ARGUMENT);
        loan_amount = UINT64_FROM_BUF(state_data_ptr + LOAN_AMOUNT_OFFSET);
        if (amount_in < loan_amount)
            rollback(SBUF("Loan: Not enough currency sent!"), TOO_SMALL);

        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        {
            maker_accid[i] = state_data_ptr[i + MAKER_ACCID_OFFSET];
            taker_accid[i] = state_data_ptr[i + TAKER_ACCID_OFFSET];
        }
        interest = UINT64_FROM_BUF(state_data_ptr + INTEREST_OFFSET);
        collateral_amount = UINT64_FROM_BUF(state_data_ptr + COLLATERAL_AMOUNT_OFFSET);

        // Prepare txs
        txs[0].receiver = role == borrower ? maker_accid : taker_accid;
        txs[0].amount = collateral_amount - interest;
        txs[0].currency = state_data_ptr[COLLATERAL_CURRENCY_OFFSET];
        txs[1].receiver = role == borrower ? taker_accid : maker_accid;
        txs[1].amount = interest;
        txs[1].currency = state_data_ptr[COLLATERAL_CURRENCY_OFFSET];
        txs[2].receiver = role == borrower ? taker_accid : maker_accid;
        txs[2].amount = loan_amount;
        txs[2].currency = state_data_ptr[LOAN_CURRENCY_OFFSET];
        txq = 3;

        if (state_set(0, 0, SBUF(loan_id)) < 0)
            rollback(SBUF("Loan: Could not write state!"), INTERNAL_ERROR);

        TRACESTR("Loan: Repay");
        break;
    case close:
        // Check if loan can be closed
        state_counter -= state_counter > 0 ? 1 : 0;
        UINT64_TO_BUF(state_counter_data, state_counter);
        if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
            rollback(SBUF("Loan: could not write state_counter"), INTERNAL_ERROR);
        if (state_data_ptr[LOAN_STATE_OFFSET] != running)
            rollback(SBUF("Loan: Loan can not be closed"), INVALID_ARGUMENT);
        if (amount_in > 1000000)
            rollback(SBUF("Loan: Too much currency sent!"), TOO_BIG);
        time = ledger_last_time();
        if (time < 1)
            rollback(SBUF("Loan: Could not retrieve last ledger time"), INTERNAL_ERROR);
        timestamp_end = UINT64_FROM_BUF(state_data_ptr + TIMESTAMP_END_OFFSET);
        if ((uint64_t)time < timestamp_end)
            rollback(SBUF("Loan: Loan period is not over yet"), INVALID_ARGUMENT);

        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
        {
            maker_accid[i] = state_data_ptr[i + MAKER_ACCID_OFFSET];
            taker_accid[i] = state_data_ptr[i + TAKER_ACCID_OFFSET];
        }

        // Prepare tx
        txs[0].receiver = role == borrower ? taker_accid : maker_accid;
        txs[0].amount = UINT64_FROM_BUF(state_data_ptr + COLLATERAL_AMOUNT_OFFSET);
        txs[0].currency = state_data_ptr[COLLATERAL_CURRENCY_OFFSET];
        txq = 1;

        if (state_set(0, 0, SBUF(loan_id)) < 0)
            rollback(SBUF("Loan: Could not write state!"), INTERNAL_ERROR);

        TRACESTR("Loan: Close");
        break;
    case resend:
        // Resend failed tx
        state_counter -= state_counter > 0 ? 1 : 0;
        UINT64_TO_BUF(state_counter_data, state_counter);
        if (state_set(SBUF(state_counter_data), SBUF(state_counter_key)) != 8)
            rollback(SBUF("Loan: could not write state_counter"), INTERNAL_ERROR);
        if (amount_in > 1000000)
            rollback(SBUF("Loan: Too much currency sent!"), TOO_BIG);
        for (int i = 0; GUARD(ACCID_SIZE), i < ACCID_SIZE; ++i)
            maker_accid[i] = state_data_ptr[i];

        // Prepare tx
        txs[0].receiver = maker_accid;
        uint64_t a = float_sto_set((state_data_ptr + 21), 8);
        txs[0].amount = float_int(a, 6, 0);
        if (state_data[20] == 1)
            txs[0].currency = 0;
        else
        {
            equal = 0;
            int c;
            for (c = 0; GUARD(MAX_CURRENCIES), c < MAX_CURRENCIES && equal != 1; ++c)
                BUFFER_EQUAL_GUARD(equal, state_data_ptr + 29, ACCID_SIZE, currencies[c], ACCID_SIZE, MAX_CURRENCIES);
            if (equal == 1)
                txs[0].currency = c - 1;
            else
                rollback(SBUF("Loan: IOU not supported."), INVALID_ARGUMENT);
        }
        txq = 1;
        if (state_set(0, 0, SBUF(loan_id)) < 0)
            rollback(SBUF("Loan: Could not reset loan"), INTERNAL_ERROR);

        TRACESTR("Loan: Resend");
        break;
    default:
        rollback(SBUF("Loan: Switch default..."), INVALID_ARGUMENT);
        break;
    }

    // Submit tx(s)
    etxn_reserve(txq);
    uint8_t emithash[KEY_SIZE];
    for (int i = 0; GUARD(MAX_TRANSACTIONS), i < txq; ++i)
    {
        if (txs[i].currency == 0) // Send XRP
        {
            unsigned char tx[PREPARE_PAYMENT_SIMPLE_SIZE];
            PREPARE_PAYMENT_SIMPLE(tx, txs[i].amount, txs[i].receiver, 10 + i, 0);
            int64_t e = emit(SBUF(emithash), SBUF(tx));
            if (e < 0)
                rollback(SBUF("Loan: Failed to emit XRP!"), e);
        }
        else // Send IOU
        {
            uint8_t amt_out[49];
            uint8_t *amt_out_ptr = amt_out;
            if (float_sto(SBUF(amt_out), SBUF(currencies[txs[i].currency]), SBUF(issuer_accids[txs[i].currency]), float_set(-6, txs[i].amount), amAMOUNT) < 0)
                rollback(SBUF("Loan: Could not dump IOU amount into sto"), NOT_AN_AMOUNT);
            uint8_t tx[PREPARE_PAYMENT_SIMPLE_TRUSTLINE_SIZE];
            PREPARE_PAYMENT_SIMPLE_TRUSTLINE(tx, (amt_out_ptr + 1), txs[i].receiver, 20 + i, 0);
            int64_t e = emit(SBUF(emithash), SBUF(tx));
            if (e < 0)
                rollback(SBUF("Loan: Failed to emit IOU!"), e);
        }
    }

    accept(SBUF("Loan: Everything worked as expected."), 1);
    return 0;
}
